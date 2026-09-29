import gzip
import io
import json
import os
import tempfile
import time

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
import duckdb
import boto3

logger = Logger()
s3 = boto3.client("s3")

TABLE_BUCKET_ARN = os.environ["TABLE_BUCKET_ARN"]
NAMESPACE = os.environ["TABLE_NAMESPACE"]
HISTORY_TABLE_NAME = "history"
CURRENT_TABLE_NAME = "current"
HISTORY_TABLE_IDENTIFIER = f'ctlg."{NAMESPACE}"."{HISTORY_TABLE_NAME}"'
CURRENT_TABLE_IDENTIFIER = f'ctlg."{NAMESPACE}"."{CURRENT_TABLE_NAME}"'

tmp_home = "/tmp/duckdb"
os.makedirs(tmp_home, exist_ok=True)
db = duckdb.connect(
    ":memory:", config={"home_directory": tmp_home, "temp_directory": tmp_home}
)


def init_db():
    db.sql(
        f"""
        CREATE SECRET (TYPE s3, PROVIDER credential_chain, REFRESH auto);
        ATTACH '{TABLE_BUCKET_ARN}' as ctlg (TYPE iceberg, ENDPOINT_TYPE s3_tables);
        CREATE SCHEMA IF NOT EXISTS ctlg."{NAMESPACE}";
        """
    )

    cols = [
        ("resourceType", "VARCHAR"),
        ("awsAccountId", "VARCHAR"),
        ("awsRegion", "VARCHAR"),
        ("resourceId", "VARCHAR"),
        ("resourceName", "VARCHAR DEFAULT NULL"),
        ("ARN", "VARCHAR DEFAULT NULL"),
        ("tags", "MAP(VARCHAR, VARCHAR) DEFAULT NULL"),
        ("configuration", "VARCHAR DEFAULT NULL"),
        ("supplementaryConfiguration", "VARCHAR DEFAULT NULL"),
        ("availabilityZone", "VARCHAR DEFAULT NULL"),
        ("resourceCreationTime", "TIMESTAMPTZ DEFAULT NULL"),
        ("configurationItemCaptureTime", "TIMESTAMPTZ"),
        ("configurationItemStatus", "VARCHAR"),
        ("configurationStateId", "BIGINT"),
    ]

    db.sql(
        f"""
        CREATE TEMPORARY TABLE dummy_cased ({",".join(f"{n} {t}" for (n, t) in cols)});
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE_IDENTIFIER}
            WITH ('format-version' = 3)
            AS (SELECT {",".join(f'{n} as {n.lower()}' for (n, _) in cols)} FROM dummy_cased) WITH NO DATA;
        CREATE TABLE IF NOT EXISTS {CURRENT_TABLE_IDENTIFIER}
            WITH ('format-version' = 3)
            AS {HISTORY_TABLE_IDENTIFIER} WITH NO DATA;
        """
    )

    # db.sql(f"""
    #     ALTER TABLE {TABLE_IDENTIFIER} SET PARTITIONED BY (bucket(8, resourcetype));
    # """)
    db.sql(
        f"""
        ALTER TABLE {HISTORY_TABLE_IDENTIFIER} SET SORTED BY (resourcetype, awsaccountid, awsregion, resourceid, configurationstateid DESC);
        ALTER TABLE {CURRENT_TABLE_IDENTIFIER} SET SORTED BY (resourcetype, awsaccountid, awsregion, resourceid);
        """
    )

    # Create staging table configuration and supplementaryConfiguration read as JSON. Duckdb does
    # not support reading variant directly from JSON.
    # Ensure columns in staging are in the same order as the real table. This may be different from
    # the dummy table if the table already existed in a different order to the one defined above
    ordered_cols = db.table(HISTORY_TABLE_IDENTIFIER).columns
    db.sql(
        f"""
        CREATE TEMPORARY TABLE staging AS (
            FROM (FROM dummy_cased SELECT {", ".join(ordered_cols)}) SELECT * REPLACE(
                configuration::JSON AS configuration,
                supplementaryConfiguration::JSON AS supplementaryConfiguration
            ),
        ) WITH NO DATA
        """
    )


init_db()

staging_struct_cols = list(zip(db.table("staging").columns, db.table("staging").dtypes))
staging_struct_fields = ", ".join(
    (f"{col} {col_type}" for col, col_type in staging_struct_cols)
)
staging_struct_type = f"STRUCT({staging_struct_fields})[]"


def staged_count():
    return db.table("staging").count("*").fetchall()[0][0]


def db_memory_usage():
    return db.table_function("duckdb_memory").sum("memory_usage_bytes").fetchall()[0][0]


def db_memory_limit():
    return db.sql(
        "SELECT parse_formatted_bytes(current_setting('memory_limit'))"
    ).fetchall()[0][0]


def read_json_into_staging(json_file: str):
    logger.debug(
        "loading object into staging",
        extra={"json_file": json_file},
    )
    try:
        _ = (
            db.read_json(
                json_file,
                columns={"configurationItems": staging_struct_type},
                # some CIs are very large, allow for up to 256 MB
                maximum_object_size=256 * 1024 * 1024,
            )
            .select("unnest(configurationItems, recursive:=true)")
            .insert_into("staging")
        )
    except duckdb.OutOfMemoryException as e:
        logger.warning(
            "ran out of memory while loading large file, falling back to manually downloading and flattening",
            extra=dict(json_file=json_file, error=e),
        )
        read_json_into_staging_unpacked(json_file)
    except Exception as e:
        logger.exception(
            "failed to load to staging", extra=dict(json_file=json_file, error=e)
        )
        raise


# Loading the nested configurationItems object directly from the S3 json can be memory intensive, as
# duckdb needs to buffer and parse the entire json file at once. In some cases for very large single
# files (large histories or snapshots) this exhausts the available memory. As a fallback we manually
# download the gzipped json, extract the configuration items array, and output it to a temporary
# line delimited json file. We then load this ndjson file directly in duckdb.
#
# This method is a fallback because generally the S3, gzip, and json parsing is more efficient and
# simpler when using the duckdb native implementations.
def read_json_into_staging_unpacked(s3_file: str):
    parts = s3_file.removeprefix("s3://").split("/", 1)
    bucket = parts[0]
    key = parts[1]

    try:
        compressed_buffer = io.BytesIO()
        s3.download_fileobj(Fileobj=compressed_buffer, Bucket=bucket, Key=key)
        compressed_buffer.seek(0)

        with gzip.GzipFile(fileobj=compressed_buffer, mode="rb") as gz:
            data = json.load(gz)

        with tempfile.NamedTemporaryFile(
            mode="w", delete_on_close=False, encoding="utf-8"
        ) as unpacked:
            for r in data["configurationItems"]:
                json.dump(
                    r,
                    unpacked,
                    ensure_ascii=False,  # allow utf-8
                    separators=(",", ":"),  # don't use any whitespace
                    check_circular=False,  # skip recursive ref check
                )
                unpacked.write("\n")
            unpacked.close()

            db.read_json(
                unpacked.name,
                columns=dict((k, str(v)) for (k, v) in staging_struct_cols),
                # some CIs are very large, allow for up to 256 MB
                maximum_object_size=256 * 1024 * 1024,
            ).insert_into("staging")
    except Exception as e:
        logger.exception(
            "failed to load to staging when using unpacked json fallback method",
            extra=dict(json_file=s3_file, error=e),
        )
        raise


def merge_into_tables():
    attempt = 1
    max_attempts = 5
    while True:
        logger.info(
            "merging staged values into history and current tables",
            extra={
                "staged_count": staged_count(),
                "attempt": attempt,
                "max_attempts": max_attempts,
            },
        )
        try:
            db.sql(
                f"""
                MERGE INTO {HISTORY_TABLE_IDENTIFIER}
                    USING (
                        SELECT DISTINCT ON (resourcetype, awsaccountid, awsregion, resourceid, configurationstateid) *
                        FROM staging
                    )
                    USING (resourcetype, awsaccountid, awsregion, resourceid, configurationstateid)
                    WHEN NOT MATCHED THEN INSERT BY NAME
                """
            )
            db.sql(
                f"""
                MERGE INTO {CURRENT_TABLE_IDENTIFIER} old
                    USING (
                        SELECT DISTINCT ON (resourcetype, awsaccountid, awsregion, resourceid) *
                        FROM staging SEMI JOIN (
                            FROM staging
                            SELECT resourcetype, awsaccountid, awsregion, resourceid,
                                max(configurationstateid) as configurationstateid
                            WHERE NOT ends_with(configurationitemstatus, 'NotRecorded')
                            GROUP BY ALL
                        ) latest USING (resourcetype, awsaccountid, awsregion, resourceid, configurationstateid)
                    ) AS new
                    USING (resourcetype, awsaccountid, awsregion, resourceid)
                    WHEN NOT MATCHED THEN INSERT BY NAME
                    WHEN MATCHED AND new.configurationstateid > old.configurationstateid THEN UPDATE BY NAME
                """
            )
            break
        except duckdb.TransactionException as e:
            if attempt < max_attempts:
                logger.warning(
                    "transaction error on merge, retrying", extra=dict(error=e)
                )
                attempt += 1
            else:
                logger.exception(
                    "transaction error on merge after max attempts, aborting"
                )
                raise e
        except:
            logger.exception("failed to merge tables")
            raise


class InsufficientTimeError(Exception):
    pass


class InsufficientMemoryError(Exception):
    pass


class AlreadyAbortedError(Exception):
    pass


class EarlyAborter:
    def __init__(self):
        self.already_aborted = False
        self.last_checked_time = None

    def reset(self):
        self.already_aborted = False
        self.last_checked_time = None

    def already_aborted_check(self):
        if self.already_aborted:
            raise AlreadyAbortedError("already aborted")

    def time_check(self, remaining_time):
        msg_fields = dict(remaining_time_ms=remaining_time)
        logger.debug("remaining time", extra=msg_fields)
        if remaining_time < 60000:
            msg = "close to timeout, skipping remaining records"
            logger.warning(msg, extra=msg_fields)
            self.already_aborted = True
            raise InsufficientTimeError(msg)

    def memory_check(self, current_memory_usage, memory_limit):
        msg_fields = dict(
            current_memory_usage_mb=(current_memory_usage / 1024 / 1024),
            memory_limit_mb=(memory_limit / 1024 / 1024),
        )
        logger.debug(
            "db memory usage",
            extra=msg_fields,
        )
        if current_memory_usage > 0.75 * memory_limit:
            msg = "close to max memory usage in staging tables, skipping remaining records"
            logger.warning(msg, extra=msg_fields)
            self.already_aborted = True
            raise InsufficientMemoryError(msg)

    def check_limits_and_maybe_abort(self, lambda_context: LambdaContext | None = None):
        self.already_aborted_check()

        # only do real checks every 5 seconds
        now = time.monotonic()
        if self.last_checked_time is not None and (now - self.last_checked_time) < 5:
            return
        self.last_checked_time = now

        if lambda_context is not None:
            remaining_time = lambda_context.get_remaining_time_in_millis()
            self.time_check(remaining_time)

        current_memory_usage = db_memory_usage()
        memory_limit = db_memory_limit()
        self.memory_check(current_memory_usage, memory_limit)


EARLY_ABORTER = EarlyAborter()


def record_handler(record: SQSRecord, lambda_context: LambdaContext | None = None):
    EARLY_ABORTER.check_limits_and_maybe_abort(lambda_context)

    msg = record.json_body

    msg_type = msg.get("messageType")
    if msg_type == "ConfigurationHistoryDeliveryCompleted":
        read_json_into_staging(f"s3://{msg["s3Bucket"]}/{msg["s3ObjectKey"]}")
    elif msg_type == "ConfigurationItemChangeNotification":
        with tempfile.NamedTemporaryFile(mode="w", delete_on_close=False) as file:
            json.dump({"configurationItems": [msg["configurationItem"]]}, file)
            file.close()
            read_json_into_staging(file.name)
    else:
        logger.warning("skipping message", extra={"messageType": msg_type})


def parse_record_for_log(record):
    try:
        msg = json.loads(record["body"])
        msg_type = msg.get("messageType")
        extra_fields = {}
        if msg_type == "ConfigurationHistoryDeliveryCompleted":
            extra_fields = {
                "s3Object": f"s3://{msg.get("s3Bucket")}/{msg.get("s3ObjectKey")}"
            }
        elif msg_type == "ConfigurationItemChangeNotification":
            item = msg["configurationItem"]
            extra_fields = {
                "resourceType": item["resourceType"],
                "awsAccountId": item["awsAccountId"],
                "awsRegion": item["awsRegion"],
                "resourceId": item["resourceId"],
            }
        return {"messageType": msg_type, **extra_fields}
    except:
        return {"messageType": "Invalid record", "record": record}


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> PartialItemFailureResponse:
    EARLY_ABORTER.reset()

    db.sql("DELETE FROM staging")

    logger.info(
        "processing config notifications",
        extra=dict(notifications_count=len(event["Records"])),
    )
    logger.debug(
        "processing config notifications",
        extra=dict(
            notifications_count=len(event["Records"]),
            notifications=[parse_record_for_log(r) for r in event["Records"]],
        ),
    )

    processor = BatchProcessor(event_type=EventType.SQS)
    with processor(
        records=event["Records"], handler=record_handler, lambda_context=context
    ):
        processor.process()

    merge_into_tables()

    return processor.response()


def local_main():
    logger.info("running local main", duckdb_build_info=duckdb.build_info())
    db.sql("show all tables").show()


if __name__ == "__main__":
    local_main()
