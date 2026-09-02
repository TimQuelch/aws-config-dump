#!/usr/bin/env bash

set -euo pipefail

mkdir -p build

uv export \
	-o requirements.txt \
	--no-dev \
	--frozen \
	--no-editable \
	--no-hashes \
	--quiet

uv pip install \
	-r requirements.txt \
	--target build \
	--python-version 3.14 \
	--only-binary ":all:"

cp ./main.py build

# duckdb_version="$(uv run python -c "import duckdb; print(duckdb.build_info()['duckdb_version'])")"
# for extension in aws httpfs iceberg avro; do
# 	echo "downloading duckdb extension: $extension"
# 	url="https://extensions.duckdb.org/v${duckdb_version}/linux_amd64/${extension}.duckdb_extension.gz"
# 	curl -sL "$url" | gunzip >"build/${extension}.duckdb_extension"
# done

(
	touch -d 20260101 build
	cd build
    # reset all timestamps
	fd -u -x touch -d 20260101

    # remove unnecessary debug info, tests, and dist info
    # fd -u -td -e dist-info -X rm -rf
    # fd -u -td tests -X rm -rf
    # fd -u -e so -x strip --strip-debug
    # fd -u -F '.so.' -x strip --strip-debug
    # fd -u -e duckdb_extension -x strip --strip-debug

	zip -r -oX - . > ../lambda.zip
)
rm requirements.txt
sha256sum <lambda.zip >&2
