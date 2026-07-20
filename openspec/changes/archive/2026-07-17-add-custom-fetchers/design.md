## Context

`build_database` (`build.rs:31`) runs three kinds of acquisition today, and only
one of them is Config:

```
  Config API ──▶ JSON lines ──▶ temp file ──▶ COPY ──▶ resources ──▶ derived tables
  (build.rs:97)                                        (THE SPINE)
  Org API ─────▶ Vec<(id, name)> ────────────────────▶ accounts       (build.rs:47)
  IAM API ─────▶ Vec<(arn, doc)> ────────────────────▶ aws_managed_policy (build.rs:58)
```

The two non-Config fetches are hardcoded `task::spawn` blocks that bypass the
spine and land in side tables. Bypassing the spine costs them the whole resource
idiom: no per-type derived table, no `resourceTypes` entry, no `acd query`
support, no completions. They work because each has a bespoke consumer written
for it (`query.rs:145`, `builtin_alterations.toml:41`).

The spine itself is built by `build_resources_table` (`db.rs:146`), which COPYs
JSON lines into `resources_temp` and MERGEs into `resources`. Two parts of that
flow encode an assumption that **Config is the sole authority over the table**:

```sql
-- db.rs:253 — deletes anything Config's identifier listing doesn't know about
MERGE INTO resources USING identifiers_temp USING (accountId, resourceType, resourceId)
    WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

```sql
-- db.rs:135 — max() over the WHOLE table drives Config's incremental cutoff
SELECT max(configurationItemCaptureTime) FROM resources;
```

Neither is scoped by `resourceType`. Any row on the spine that Config didn't put
there gets deleted by the next incremental build, and any row with a fresh
timestamp drags Config's cutoff forward and causes Config to under-fetch. These
two facts constrain the design more than anything else.

The long-term intent (not this change) is that `accounts` and
`aws_managed_policy` migrate onto this mechanism. Their requirements are
therefore treated as design constraints here even though no code moves.

## Goals / Non-Goals

**Goals:**
- One seam for acquiring non-Config data, general enough to absorb the two
  hardcoded fetches later without redesign.
- Fetched resources become first-class: queryable, derived-tabled, completed,
  with no per-fetcher downstream plumbing.
- Fetchers own their own freshness/incrementality — the pipeline does not
  impose one model.
- Config's control plane stops silently corrupting non-Config rows.
- OUs available as a queryable resource type.

**Non-Goals:**
- User-declared external-command fetchers (`[[fetchers]]` in `config.toml`).
  The trait accommodates it as one more impl. Configuring the *builtin* fetchers
  does ship here (see Decisions) — declaring new ones in config does not. The two
  are different axes and only the second is deferred.
- Migrating `accounts` / `aws_managed_policy`. Design constraints only.
- Changing the `resources` schema. Fetchers conform to it as-is.
- Reworking the `build_derived_tables` → `apply_schema_alterations` →
  `build_custom_tables` phase ordering (see Risks).
- Incremental fetching for OUs. Orgs are small; full refetch every build.

## Decisions

**Fetchers land on the `resources` spine, not in side tables.**
The alternative — a side table per fetcher, following the `accounts` /
`aws_managed_policy` precedent — was rejected. It requires bespoke downstream
plumbing per fetcher (a consumer written by hand for each), which is exactly the
cost this change exists to eliminate. Landing on the spine makes
`organizations_organizationalunit` appear as a derived table for free, and OU
tags land in the existing `tags MAP(VARCHAR, VARCHAR)` column. The cost is
conforming to a schema shaped for Config items and inheriting its lifecycle,
which the type-claiming decision below contains.

Note this does *not* preclude side tables later: a fetcher lands raw rows on the
spine, and a builtin `custom_table` derives a friendlier shape on top. That
acquisition/derivation split is how `accounts` and `aws_managed_policy` can
eventually be reproduced — fetcher underneath, derivation on top preserving the
table name their consumers expect.

**A fetcher gets the connection pool, not just an output channel.**
The obvious trait — `async fn fetch(&self) -> impl Stream<Item = Row>` — is
enough for OUs and would be a trap. `build_aws_managed_policies_table`
(`db.rs:20`) does a two-phase fetch with a **DB-state-dependent diff in the
middle**: list all `(arn, defaultVersionId)` cheaply, read the existing table,
diff by version, then fetch documents only for what changed. Designing the trait
around OU — which needs nothing but a profile and a sender — produces something
that can never absorb managed policies. So:

```rust
trait Fetcher {
    fn name(&self) -> &str;                  // "organizational_units"
    fn resource_types(&self) -> &[&str];     // types claimed on the spine
    fn semantics(&self) -> Semantics;        // Authoritative | Merge
    async fn fetch(&self, ctx: &FetchContext) -> anyhow::Result<()>;
}

struct FetchContext {
    profile: Option<String>,
    db: ConnectionPool,          // managed policies needs this
    rows: mpsc::Sender<String>,  // JSON lines onto the spine
    progress: MultiProgress,
}
```

**Everything is testable without network access or credentials.**
Development and CI have neither, so no test may reach AWS and no verification step
may assume a live organization. This constrains the structure rather than just
the test suite, in two places.

*The AWS calls and the row mapping live in different crates.* `aws-smithy-mocks`
is a dev-dependency of `aws-client` only (`aws-client/Cargo.toml:67`), so AWS
calls must be mocked where they are made. OU fetching follows the split
`org_client` already uses — `fetch_org_accounts(profile)` constructs the client
and delegates to `fetch_org_accounts_inner(client)`, which is what the mock tests
target (`org_client.rs:21`). The `Fetcher` impl in `acd-cli` then only maps plain
returned data to JSON lines, testable with hand-built fake values and no AWS
involvement at all. `acd-cli` gains no new dev-dependency.

*The fetcher driver is generic, not a global registry.* Tests need to inject a
fake fetcher — one that emits canned rows, fails on demand, or records whether it
ran — so the driver is generic over the trait:

```rust
pub async fn fetch_rows<F: Fetcher>(
    fetcher: &F,
    pool: &ConnectionPool,
    profile: Option<String>,
    progress: &MultiProgress,
) -> anyhow::Result<Option<FetchOutput>>;
```

Alternatives rejected: a `LazyLock` registry mirroring `builtin_alterations.rs`
cannot have a fake substituted, and the mirror is false anyway — that module holds
*data* (SQL strings), which is why `build_custom_tables` can take `&[CustomTable]`
and let a test pass a synthetic one (`db.rs:646`). Fetchers are behaviour, so the
trick doesn't transfer. `Vec<Box<dyn Fetcher>>` doesn't compile as written:
`async fn` in a trait is RPITIT and not dyn-safe, and `async-trait` is not a
dependency. Enum dispatch over concrete impls is the house answer to exactly that
problem (`DispatchingClient`, `config_client.rs:114`), but it would need a
test-only variant inside a production enum to allow fakes. A generic driver needs
none of it.

With one builtin fetcher, the "registry" is therefore a short list in `build.rs`,
not machinery. Enum dispatch can arrive with the second fetcher, which is when it
starts paying for itself; `fetch_rows`' signature doesn't change when it does.

*The driver is a free function rather than a method on the trait.* This is a
separate axis from the one above, and testability does **not** decide it: a
provided trait method would be just as generic over `Self` and just as
fake-friendly. Generic-ness is what rules out the registry and `dyn`; it says
nothing about where the driver lives.

What decides it is that `fetch_rows` is the sole enforcement point of the
soft-fail invariant — `Err` from a fetcher becomes `Ok(None)`, so a transient API
error lands nothing instead of being read as "this type is now empty" and having
`Authoritative` semantics delete every row for it. Rust has no `final` on trait
methods, so as a provided method that invariant would be advice an implementer
could override, silently, while still satisfying the trait. As a free function it
is the only route to a `FetchOutput`, which is the only thing
`land_fetched_rows` accepts. Given this interaction is the most dangerous one in
the design — it surfaced three separate times from three different directions
during planning — it is worth making unoverridable rather than merely provided.

Secondary: a trait carrying both `fetch` (implement this) and `fetch_rows` (call
this, never implement it) is a confusing surface, and the split keeps "what a
fetcher is" apart from "how fetchers are run".

The costs are real and accepted. `fetch_rows(&f, ..)` reads worse than
`f.fetch_rows(..)` and is less discoverable. And the seal is shallow rather than
airtight: `FetchOutput`'s fields are `pub` within the crate, so a determined
implementer could still construct one that never went through the check. It
raises the floor, it doesn't weld the door shut.

**Builtin fetchers are configured per database, keyed by fetcher name.**
The Organizations API must be called against the management or delegated-admin
account. That's routinely *not* the profile Config data is read with — an
aggregator in a tooling account, say — so a fetcher hardwired to the database's
`aws_profile` is unusable on exactly the cross-account estates it's most useful
for. And a database whose credentials have no Organizations access at all wants
the fetcher off entirely rather than soft-failing on every build.

```toml
[databases.prod]
aws_profile = "prod-readonly"

  [databases.prod.fetchers.organizational_units]
  enabled = true
  aws_profile = "prod-management"
```

Per-database rather than global because that's where `aws_profile` and
`aggregator_name` already live, and because two databases can point at different
organizations — a single global fetcher profile would be wrong for one of them.
Keyed by fetcher name (`Fetcher::name()`, which already exists) rather than by
OU-specific fields on `DbConfig`, so the second builtin fetcher needs no new
config shape.

An unset profile falls back to the database's `aws_profile`, and an unset
`enabled` is true, which keeps the whole thing purely additive: every existing
config file behaves exactly as it does today.

Disabling by name mirrors `builtin_alterations`' `disable_all`/`disabled`
(`config.rs:65`) in spirit, but not in form: that list is global and these
settings can't be, and a per-fetcher table carries the profile too rather than
needing a second mechanism beside it.

**A disabled fetcher stops claiming its resource types.**
This falls out of the claim being what shields fetched rows from Config's
identifier delete, and it's the one part of this that isn't obvious.

`--no-fetch` and `enabled = false` mean different things, and the claim is where
the difference shows. `--no-fetch` is transient — "don't fetch on this
invocation" — so claims persist and the previous rows survive untouched. But
`enabled = false` is durable: this database does not want OUs. Keeping the claim
there would freeze whatever rows were last fetched into the table forever, stale
and unrefreshable, clearable only by `--rebuild`. Dropping the claim hands them
back to Config's delete, which reaps them on the next `Api` build.

So disabling a fetcher removes its data, on a lag of one API build. That is the
same self-healing path the orphan risk below already describes for a fetcher
deleted from the registry, and it's what the setting plainly says: rows the user
has declared they don't want get cleaned up rather than rotting in place.

**`resource_types()` is declared up front, and scopes Config's control plane.**
This is the linchpin. Because the claimed types are known *before* any fetching,
both offending queries can be narrowed:

- the identifier delete (`db.rs:253`) gains `AND resourceType NOT IN (<claimed>)`
- `get_timestamp_cutoff` (`db.rs:135`) gains `WHERE resourceType NOT IN (<claimed>)`

Config stops being the authority on *all* rows and becomes the authority on
*Config's* rows. Alternative considered: have each fetcher also emit identifier
rows so the existing delete just works. Rejected — it makes every fetcher
participate in a protocol built for Config's listing semantics, and doesn't fix
the cutoff bug at all.

**Two freshness modes cover all three known fetchers.**
`Authoritative` = "my rows are the complete set for my claimed types" → delete
the claimed types then insert (OUs, accounts: small, always fully refetched).
`Merge` = "insert/update only, never delete" (managed policies: a cache built up
over builds). A third mode for Config-style identifier-driven deletion was
considered and rejected — Config keeps its existing bespoke path; the point is
to serve the fetchers, not to re-express Config as one.

**The claimed-type set comes from the in-memory registry; `custom_resource_types` is left alone.**
Both consumers of the set — the scoped delete and the scoped cutoff — run at
build time, where the compiled-in registry is trivially available
(`registry().flat_map(|f| f.resource_types())`). No schema change, correct by
construction for the build that's running.

Persisting the set in `custom_resource_types` (`db.rs:395`) was considered, since
it already exists for exactly this job — a registry of non-Config types, unioned
into the `resourceTypes` view. Rejected for now on two grounds. First, ownership:
`build_custom_tables` opens with `DELETE FROM custom_resource_types`
(`db.rs:455`) and repopulates it from config, so fetcher types written there
would survive or not depending on phase ordering. Reuse means untangling that
ownership, not just an `INSERT`. Second, nothing needs it yet: `resourceTypes`
already picks up fetcher types through its `SELECT DISTINCT resourceType FROM
resources` branch, so OUs reach queries and completions with no registration at
all.

The known cost is that a claimed type with *zero* rows is invisible — it drops
out of completions until something fetches a row for it. Cosmetic, and it can't
bite the OU fetcher on a real organization. It gets sharper if user-declared
fetchers ship, where types appear and disappear with config edits rather than
recompiles, and where the orphan-reaping risk below stops being theoretical.
That is the point at which persistence starts earning its keep — deciding it now
on OU's evidence alone would be optimizing for the easy case.

**Fetchers write to their own temp file, not Config's `resources_tx`.**
Fanning into the existing sender (`build.rs:159`) is tempting since
`fetch_resources` already multiplexes producers into it, but Config's file flows
through the identifier-delete MERGE, which fetcher rows must not enter. Separate
file per fetcher keeps each fetcher's merge semantics its own, and keeps a slow
or failing fetcher from holding Config's temp-file writer open.

**Fetchers run concurrently with the Config fetch and soft-fail.**
Preserves the existing property that org-accounts and managed-policies fetch in
parallel with Config (`build.rs:47`, `build.rs:58`). A fetcher failure logs and
is skipped, leaving prior rows intact rather than failing the build — matching
`list_aws_managed_policies`'s existing `.unwrap_or_default()` (`build.rs:67`)
and `build_custom_tables`' log-and-continue (`db.rs:507`). A build that dies
because one optional OU fetch 403'd would be a regression in practice.

**Fetchers run under `Api` and `Snapshots`, never under `Skip`.**
Fetcher lifecycle is orthogonal to *where Config data comes from* — a snapshot
contains no OUs either, so `Snapshots` builds need fetchers just as much as `Api`
builds do. But it is not orthogonal to *whether the build fetches at all*.
`FetchSource::Skip` comes from `--no-fetch`/`-n`, documented as "Don't fetch
data, only build the resource tables" (`cli.rs:57`); running fetchers under it
would contradict the flag's stated contract and make an offline build hit the
network. `Skip` means skip.

`Snapshots` needs care: `build_resources_table_from_snapshots` (`db.rs:305`)
does `CREATE OR REPLACE TABLE resources`, so fetchers must land *after* it, not
concurrently.

Note this puts the new mechanism at odds with the two fetches it will eventually
absorb: `org_accounts_handle` (`build.rs:47`) and `managed_policy_handle`
(`build.rs:58`) are both spawned *before* the `match fetch_source`, so `acd build
--no-fetch` calls the Organizations and IAM APIs today. See Migration Plan.

**OU rows use `AWS::Organizations::OrganizationalUnit`.**
The real CloudFormation type name, not a `Custom::`-prefixed one, so it reads
naturally in queries and matches the sibling `AWS::Organizations::Policy` rows
Config already records (`db.rs:281`). Verified against a real database: Config
records neither `AWS::Organizations::OrganizationalUnit` nor
`AWS::Organizations::Account`, so the claim doesn't collide — and the deferred
accounts migration has a free type name waiting for it too.

`accountId` is the management account ID from `DescribeOrganization`, keeping
`--accounts` filtering and the `accounts` LEFT JOIN (`query.rs:145`) coherent.
`awsRegion` is `'global'`, matching what Config already stores for other global
resource types — consistency with the rows already in the table beats
consistency with a principle, and it means the `regions` view (`db.rs:397`) gains
no new value on OUs' account. The parent OU/root ID goes in `configuration` — the
tree is flattened, and the `/Root/Prod/Workloads` path is a derivation (recursive
CTE in a builtin alteration) rather than something the fetcher computes.
Acquisition stays dumb; derivation stays in SQL.

## Risks / Trade-offs

- **[Risk]** A fetcher claiming a type Config also records would authoritatively
  delete Config's rows for it. Not a live problem — Config records neither type
  the builtin fetchers want (verified) — but it is a **latent** one: AWS adds
  Config resource-type support over time, so a type that's free today can start
  arriving from Config later, silently turning `Authoritative` semantics into
  data loss with no code change on our side. → **Mitigation**: have the registry
  refuse to start when a claimed type appears in Config's `get_resource_counts`
  output, turning a silent data-loss bug into a startup error. Cheap, since the
  counts are already fetched (`build.rs:133`).
- **[Risk]** The spine is shaped for Config items; OUs must leave fields that
  don't apply (`configurationItemStatus`, `configurationStateId`,
  `resourceCreationTime`) empty. → **Mitigation**: NULL rather than inventing
  sentinel values, so nothing downstream mistakes a placeholder for real data.
  `awsRegion` is the exception — `'global'` is not an invented sentinel but the
  value Config itself already uses for global types, so OUs join and filter
  alongside the global rows already present. Accepted cost of the spine decision.
- **[Risk]** **Mock drift.** With no live testing, every OU test asserts against
  mocked Organizations responses — so the suite validates our *belief* about the
  API's pagination and tree shape, and passes just as green if that belief is
  wrong. The recursive `ListOrganizationalUnitsForParent` walk is where this
  bites: a mock returning `next_token` only at the root would hide a fetcher that
  silently truncates nested pages, and every offline assertion would still pass.
  → **Mitigation**: none available offline; this is a real, accepted gap rather
  than something to design around. Reduce exposure by mocking pagination *at a
  nested level* rather than only at the root (task 5.5), and by keeping the
  fetcher's AWS surface to the smallest set of calls that does the job. First run
  against a real organization is the actual test, and is expected to be the point
  where shape bugs surface.
- **[Risk]** `configurationItemCaptureTime` on fetcher rows is a fabricated
  `now()`. Even with the cutoff scoped to Config types, it's a real-looking
  column with a synthetic value that users will reasonably read as "when AWS
  recorded this". → **Mitigation**: document it as fetch time; it is genuinely
  when `acd` observed the row.
- **[Risk]** Scoping the delete and cutoff by claimed type means the claimed-type
  set must be correct at *every* build. If a fetcher is removed, its old rows
  become unreachable orphans — no longer claimed, so Config's delete now reaps
  them (arguably correct, but surprising and silent). → **Mitigation**: accept
  for now; the reaping self-heals rather than corrupting. Worth revisiting if
  user-declared fetchers ship, where fetchers appear/disappear with config edits.
- **[Trade-off]** The `db: ConnectionPool` in `FetchContext` is unused by the OU
  fetcher and by any future external-command fetcher. It exists solely so the
  managed-policy migration is possible later. Carrying it now is deliberate: the
  alternative is discovering the trait can't express managed policies after
  fetchers exist and the trait is load-bearing.
## Migration Plan

Purely additive. No existing table, flag, or query behavior changes; the delete
and cutoff scoping are no-ops until a fetcher claims a type. Rollback is
removing the OU fetcher from the registry — though note the orphan-reaping risk
above means its rows are then deleted on the next build, which is the intended
cleanup rather than a failure mode.

**The deferred `accounts` / `aws_managed_policy` migrations are not pure
refactors, because of the `Skip` decision above.** Both fetches are spawned
unconditionally before `match fetch_source` (`build.rs:47`, `build.rs:58`), so
`acd build --no-fetch` calls the Organizations and IAM APIs today. Moving them
onto the registry makes them honour `--no-fetch` and stop. That aligns them with
the flag's documented meaning and is almost certainly a bugfix — but it is a
user-visible behavior change (a `--no-fetch` build would no longer refresh
managed policy documents), so it should be a deliberate part of that migration
rather than a side effect noticed afterwards.

The deferred `accounts` / `aws_managed_policy` migrations will hit a
**phase-ordering landmine** that this change does not create but must not make
worse. Current order (`build.rs:119-121`):

```
build_derived_tables ──▶ apply_schema_alterations ──▶ build_custom_tables
                                   ▲
                    alterations DEPEND on aws_managed_policy
                    (builtin_alterations.toml:41)
```

If `aws_managed_policy` becomes a custom table derived from spine data, it is
built in the **last** phase — after the alterations that depend on it, whose
dependency check would then silently skip them (`db.rs:472` logs "missing
dependencies" and continues). That migration needs its derivation to land in
`build_derived_tables`, or needs the phases reworked into a dependency-driven
order. Flagged here so the fetcher seam isn't designed in a way that assumes the
current ordering is fine.

## Verification Gaps

Everything ships verified offline (178 tests, no network, no credentials). Three
things that testing structurally cannot establish, recorded so the first live
`acd build` is treated as the real test rather than a formality:

- **The Organizations API's actual shape.** Every OU test asserts against mocked
  responses, so the suite validates our *belief* about pagination and tree
  structure. Pagination is mocked at a nested level rather than only at the root
  (the failure mode that would otherwise hide a truncating walk), but a mock that
  is wrong in the same way the code is wrong stays green forever.
- **Real tree depth and OU counts.** The walk is breadth-first with one
  `ListOrganizationalUnitsForParent` call per OU plus one `ListTagsForResource`
  per OU, so call count grows linearly with the tree. Fine for a normal
  organization; unmeasured against a large one.
- **The credential path.** `sdk_config::load_config` is never exercised for real,
  so profile resolution and permissions (`organizations:ListRoots`,
  `ListOrganizationalUnitsForParent`, `ListTagsForResource`,
  `DescribeOrganization`) are untested. A missing permission surfaces as the
  soft-fail path: logged, build continues, no OU rows.

Two bugs were caught by writing these tests that live testing would have found
more slowly and more expensively:

- `supplementaryConfiguration` emitted as `null` rather than `{}` silently cost
  OUs their derived table. `build_resource_derived_table` unnests that column,
  `UNNEST` rejects an all-null JSON column, and the resulting error is logged and
  swallowed (`db.rs`) — so OUs would have landed on the spine, queried fine, and
  simply had no `organizations_organizationalunit` table, with the reason buried
  in a log line.
- DuckDB binds `->>` looser than `LIKE`, so
  `resourceType = 'X' AND configuration->>'ParentId' LIKE 'r-%'` misparses into
  indexing JSON by a boolean. Only bites when chained after another AND-ed
  comparison, which is why it slipped through an isolated check. Extractions are
  parenthesised as a result.

## Open Questions

None outstanding. The type-collision check, the `awsRegion` value, `--no-fetch`
behavior, and where the claimed-type set lives are all decided above. The
deferred work — user-declared fetchers, and the `accounts` /
`aws_managed_policy` migrations — carries its own open questions, notably
whether the claimed-type set needs persisting once fetchers can appear and
disappear with config edits.
