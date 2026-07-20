## Context

`OrganizationalUnitsFetcher::fetch` currently does this:

```
fetch_management_account_id()              1 call
fetch_organizational_units()               ← all the real work, no reporting
    list_roots                             ·  1 call
    BFS per level, buffer_unordered(8)     ·  concurrent within a level
    tag pass, buffered(8) + zip            ·  concurrent, ordered
  ↓ returns Vec<OrganizationalUnit>
progress_bar("...", ous.len())             ← total first knowable here
for ou in &ous { send; bar.inc(1) }        ← pure CPU, fills instantly
```

The concurrency arrived with `f5ab665` and made the fetch much faster, but it did
not touch the reporting defect — it made the collect finish sooner, not report
sooner. The bar is still created after every network call has returned, and still
measures JSON serialization. `ous.len()` is still unavailable until the walk
finishes, so the bar's need for a total is still what forces the collect.

This is worth stating plainly because it is the easy thing to get wrong when
reading the two changes together: **concurrency and eagerness are independent
axes.** The walk is now concurrent and still fully collecting. Only eager
production past a channel lets the total lead the position.

`config_client` does not have this problem: `build.rs` calls
`get_resource_counts()` — a cheap count API — and passes a real total to
`util::progress_bar` before fetching begins. Organizations has no count endpoint,
and IAM has none for the managed-policy fetcher that follows. So this is a shape
that has to be designed rather than copied.

Constraints: no new dependencies if practical; clippy pedantic; prefer async
throughout.

## Goals / Non-Goals

**Goals:**
- The bar's position advances against real Organizations calls.
- The bar's total grows as the BFS enqueues OUs; a total known up front is not
  required.
- The total leads the position, so the bar is not pinned near 100%.
- Establish the shape the managed-policy fetcher will follow.
- Keep the emitted rows, the OU row contract, and soft-fail behavior identical.

**Non-Goals:**
- Extracting a reusable pipeline helper into `fetcher.rs`. See "Decisions".
- Changing `Fetcher`, `FetchContext`, or `fetch_rows`. `ctx.rows` and
  `ctx.progress` are already exactly what this needs — that the trait needs no
  change *for this reason* is evidence it is right and only the OU
  implementation is wrong. (`harden-fetcher-seam` has since changed the trait
  for an unrelated reason — dyn-compatibility — so `fetch` returns a boxed
  `FetchFuture`. This change is written against that signature and does not move
  it further. Practical consequence: everything the stream holds across an await
  must be `Send`.)
- Reducing memory. OUs are small and land in a temp file (`fetcher.rs`);
  streaming buys honest reporting and overlap, not footprint.
- Ordering guarantees on emitted rows. Nothing downstream depends on OU order.

## Decisions

### Discovery must be eager, not a lazy stream

This is the load-bearing decision, and the reason the obvious design fails.

A lazy `Stream` is pulled by its consumer. With enrichment running 8-wide, the
consumer only pulls when it has a free slot, so discovery only runs when
enrichment demands more:

```
lazy:   BFS ──(pull)──▶ count ──(pull)──▶ buffer_unordered(8) ──▶ emit
                          │                      │
                   raises total            gated by this
                   only on demand ◀─────────────┘

        ⇒ total ≈ position + 8, always. Bar pinned near 100%.
```

No item shape fixes this — not a stream of futures, not an event enum. A lazy
stream's discovery is gated by its consumer's demand, full stop. Only eager
production decouples them:

```
eager:  BFS task ──▶ mpsc(256) ──▶ consumer ──▶ buffer_unordered(8) ──▶ emit
           │                                          │
     raises total on enqueue,                   raises position
     at full speed                              on completion

        ⇒ total leads. Bar means something.
```

So the OU stream is a producer task pushing through a bounded channel, with the
consumer wrapping the receiver. This mirrors `config_client`, which joins
an eager `list_fut` and a `get_fut` across a 256-capacity channel — the same
structure, adopted there for throughput rather than for reporting.

**Alternative considered:** lazy `stream::unfold` over the BFS frontier. Rejected
above — cannot let the total lead. Also awkward to write.

### The progress bar is passed into `org_client`

Because the total must rise where the BFS enqueues, and that is inside
`aws-client`, something has to report from there. Options:

| Option | Cost |
|---|---|
| **Pass the `ProgressBar` down** | `indicatif` on the Organizations path |
| Callback `on_discovered: impl Fn(u64)` | avoids the dep; leaves a progress-shaped hole in the signature anyway |
| Greedy "pump" stage in `acd-cli` draining to an unbounded queue | preserves layering; costs a task and unbounded buffering, and exists purely to relocate one line |

**Decision: pass the bar down.** The layering objection is already spent —
`indicatif` is an `aws-client` dependency today and `config_client`
takes a `ProgressBar` for exactly this purpose. The pump is an elaborate way to
pretend otherwise, and the callback is the same coupling wearing a disguise.
Consistency with the sibling client wins.

### Tags are attached inside the stream

The stream yields fully-tagged `OrganizationalUnit` values; callers cannot hold
an untagged one and so cannot forget to tag it. This is possible only because
eagerness — not item shape — is what solved the reporting problem: with a lazy
stream the caller would have had to observe discovery and completion as separate
events, forcing the tag fetch out into the open as a stream of futures.

**Deferred:** the managed-policy fetcher needs to consult the database between
discovery and enrichment, to skip fetching documents whose stored version is
current. That wants items carrying metadata alongside lazy per-item work
(`Pending { meta, work }`), where dropping an unpolled future *is* the skip. That
is an extension of this skeleton, not a different one, and it is not built here.

### No pipeline helper yet

A `fetcher.rs` helper taking `(discovery, enrich, to_row)` and owning the bar is
tempting for the "general shape". It is deliberately not built: it would be an
abstraction over a sample of one, and would bake in "every discovered item gets
enriched" — which the policy fetcher's database filter immediately violates.
Hand-compose here, write the policy fetcher second, extract once both have
exercised the shape.

### Concurrency

- BFS: each level's children are already listed concurrently through
  `buffer_unordered(CONCURRENCY)`, with `CONCURRENCY = 8` a named constant in
  `org_client`. This change preserves that and moves it into the producer task.
  It matters beyond speed: serial discovery could not outrun 8-wide enrichment,
  which would leave the bar pinned near 100% regardless of eagerness.
- Tags: 8-wide, matching `config_client.rs`'s `for_each_concurrent(8)`, reusing
  the same `CONCURRENCY` constant rather than introducing a second one.
- Tag ordering: the current tag pass uses ordered `buffered` and zips results
  back onto the OU vector positionally, so ordering is load-bearing — an
  unordered pass there would misattribute every tag set. Inside the stream that
  hazard disappears, because each OU is enriched with its own tags and the two
  are never re-paired by position. `buffer_unordered` is therefore correct here
  even though it would be a bug in the code being replaced. This distinction
  must survive review: it looks like a regression against `harden-fetcher-seam`
  task 5.3 and is not one.

### Client handle

`org_client` gains a handle type holding an `aws_sdk_organizations::Client`, so
the pipeline can call it more than once. Today's free functions each call
`load_config(profile)` and build their own client. The existing
`fetch_organizational_units_inner(client)` mock seam becomes methods on the
handle; `mock_client!` continues to work.

## Risks / Trade-offs

- **The bar reads oddly at the very start** — with 3 discovered and 3 done it
  renders `3/3`, then jumps to `3/24`. → Inherent to growing totals; mitigated by
  concurrent discovery outrunning enrichment, so the lead establishes quickly.
  Accepted: it beats a bar that reports `0/N` for the entire fetch.

- **Backpressure can stall discovery** — a bounded channel means a stalled
  consumer eventually blocks the BFS and the total stops leading. → Capacity 256,
  matching `config_client`; ample runway for an organization's worth of OUs.

- **Test coverage moves** — `aws-client` tests currently cover the whole walk
  including tags (the `fetch_organizational_units_*` mock tests). The pipeline is harder to assert on
  than a returned `Vec`. → Keep the existing mock scenarios, collecting the
  stream to a `Vec` to assert the same facts (nesting depth, parent recording,
  nested pagination, tags, empty org). Add tests that a bar's total rises above
  its position during a fetch, and that a mid-stream failure surfaces as an error.

- **Concurrency reorders emitted rows** — nothing depends on OU order, and
  `Authoritative` semantics replace the whole set. Existing `ou_to_json_line`
  tests are per-row and unaffected.

- **A partial stream must not be mistaken for a complete one** — the streaming
  rewrite must keep errors propagating out of `fetch`, so `fetch_rows` returns
  `Ok(None)` and lands nothing. Rows already written to the temp file are
  discarded with it. → Covered by the `custom-fetchers` scenario "Prior rows
  survive a fetcher error"; asserted explicitly. `harden-fetcher-seam` reinforced
  this from the other end: spool write and flush failures now propagate too, and
  `Authoritative` landing is transactional, so neither a broken stream nor a
  broken spool can present as a short-but-complete row set.

- **The producer task must not swallow a failure into a clean end-of-stream** —
  streaming adds a failure mode the collecting version did not have. A `Vec`
  return made truncation impossible to miss; a channel that simply closes looks
  exactly like a finished walk, and under `Authoritative` semantics a silently
  truncated tree is a mass delete. → Producer errors are sent as `Err` items and
  a `JoinError` is surfaced rather than dropped (tasks 2.3, 5.3). This is the
  single highest-risk aspect of the change and the reason `harden-fetcher-seam`
  task 6.5 should not be considered redundant with it.

## Migration Plan

Internal refactor: no schema, CLI, configuration, or row-shape change. The
`organizational-units` and `custom-fetchers` specs are unchanged and their
scenarios are the regression suite. Rollback is a revert.

## Open Questions

- Concurrency of 8 is taken from `config_client` rather than measured. Tag calls
  are cheap and Organizations throttles aggressively; if throttling appears, this
  is the dial. ~~Worth leaving as a named constant.~~ Resolved: `f5ab665` made it
  the `CONCURRENCY` constant in `org_client`, documented with why it is bounded.
  Still unmeasured.

- Whether this change or `harden-fetcher-seam` lands the distinct-tag fixtures
  (`harden-fetcher-seam` task 6.4). The existing OU fixtures give every OU the
  same tags, so they would pass even if concurrent enrichment misattributed every
  result — which means neither change's tag concurrency is actually covered
  today. Whichever lands first should do it; if this change lands first, task 5.1
  carries it.
