# Shopping Sessions engine

Builders Buildathon, Databricks "Shopping Sessions" track. A single multi-threaded Rust
binary that implements `rules.md` exactly: it reads `/data`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet.

How it works:

1. All CSV files are memory-mapped and split into line-aligned chunks that worker threads parse
   with a hand-written parser for the `rules.md` dialect (quotes, `\r`, malformed lines).
2. Events are scattered into partitions by a 96-bit fingerprint of `event_id`; each partition is
   deduplicated with an open-addressing table (smallest `ingest_ts` wins, before validation).
3. Links are joined with union-find; each person is named by the byte-wise smallest ID.
4. Surviving events and valid clicks are scattered into partitions by person, sorted, sessionized,
   and attributed with a sliding 7-day click window (credit: latest click, ties by smallest
   `click_id`; assists counted per campaign activity interval).
5. A minimal built-in Parquet writer (PLAIN, uncompressed) writes both tables.

Open-source libraries used: `memmap2`, `memchr`, `xxhash-rust` (all MIT/Apache-2.0).
All other code was written by our team during the buildathon.

License: MIT.
