# Shopping Sessions engine

Builders Buildathon, Databricks "Shopping Sessions" track. A single multi-threaded Rust
binary that implements `rules.md` exactly: it reads `/data`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet.

How it works:

1. All CSV files are memory-mapped and split into line-aligned chunks. Worker threads find line
   and field boundaries 64 bytes at a time with AVX2 comma/newline/quote bitmasks, and check the
   quoting rules with a popcount; timestamps use a small month table plus SWAR digit decoding,
   with exact slow paths for anything unusual.
2. Link files are parsed first by all threads; one thread then runs a prefetching union-find
   (each person named by its byte-wise smallest ID) while the others parse events and clicks.
3. Events are scattered into 512 partitions by a 128-bit `event_id` hash (64-bit key, partition
   from independent bits) into per-thread arenas; each partition is deduplicated with a
   cache-resident open-addressing table (smallest `ingest_ts` wins, before validation).
4. Surviving events and valid clicks are scattered into person partitions; each partition groups
   rows per person with a counting sort, then sessionizes and attributes with a sliding 7-day
   click window (credit: latest click, ties by smallest `click_id`; assists counted per campaign
   activity interval, split by day).
5. A minimal built-in Parquet writer (PLAIN, uncompressed) writes one user_daily file per thread.
6. Big buffers use huge pages; memory and input mappings are released in parallel before `_exit`.

Open-source libraries used: `memmap2`, `memchr`, `xxhash-rust`, `libc` (all MIT/Apache-2.0).
All other code was written by our team during the buildathon.

License: MIT.
