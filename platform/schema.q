/ platform/schema.q — nano_tick KDB-X trade schema (RDB / writer role)
/ ====================================================================
/ Primary tick store for the project (see KDBX_SETUP.md). Defines the `trade`
/ table and the write path into a date-partitioned, splayed HDB.

/ ROLE: this process is the RDB (real-time database) / writer. It holds the
/ current day's ticks in memory and appends COMPLETED days to the HDB on
/ rollover. It does NOT load the HDB into itself: in kdb a table name is either
/ in-memory OR partitioned-on-disk, not both. Query history from a SEPARATE
/ session:  q <hdb-path>   (that loads the partitioned `trade`). This RDB/HDB
/ split is standard kdb; kdb+tick formalises it later.

/ Run:  q platform/schema.q -hdb /path/to/nano_tick_hdb
/ (default HDB: $HOME/nano_tick_hdb)

/ --- config ----------------------------------------------------------
args:.Q.opt .z.x;
hdbpath:$[`hdb in key args; first args`hdb; getenv[`HOME],"/nano_tick_hdb"];
HDB:hsym `$hdbpath;

/ --- schema (in-memory / RDB) ----------------------------------------
/ Types match the normaliser output (ingest/src/normalisers/binance.py).
/ Timestamps are kdb `timestamp` (nanos); raw Binance epoch-millis are
/ converted on insert (see ms2ts / insertRaw).
trade:([]
  time      :`timestamp$();  / trade_ts — exchange trade time; primary key
  sym       :`symbol$();     / instrument, e.g. `BTCUSDT
  venue     :`symbol$();     / e.g. `binance
  tradeID   :`long$();       / venue trade id; monotonic per sym (dedup key)
  price     :`float$();
  qty       :`float$();
  eventTime :`timestamp$();  / event_ts — venue emit time
  buyerMaker:`boolean$());   / Binance `m`: true if buyer is the maker

/ --- helpers ---------------------------------------------------------
/ epoch-millis (long) -> kdb timestamp. kdb epoch is 2000.01.01, so add the
/ nanos onto the 1970 literal directly.
ms2ts:{1970.01.01D00:00:00 + 1000000j * x};

/ Insert a batch from the pykx bridge. `rows` is a table with the same columns
/ as `trade` but time/eventTime as raw epoch-millis longs; convert + append.
/ Keeping the conversion server-side lets the Python side stay dumb.
insertRaw:{[rows]
  `trade insert update time:ms2ts time, eventTime:ms2ts eventTime from rows;
  count rows };

/ Tickerplant-style hook, so migrating to kdb+tick later needs no bridge change.
upd:{[t;x] t insert x};

/ --- persistence -----------------------------------------------------
/ Append the in-memory `trade` to the HDB as partition `dt` (a date): splayed,
/ time-then-sym sorted, with the `p#` attribute on sym (all via .Q.dpft), then
/ clear the RDB. Call at date rollover for the COMPLETED day.
savedown:{[dt]
  if[0=count trade; :0];
  `time xasc `trade;               / time order; .Q.dpft's sym sort is stable
  .Q.dpft[HDB; dt; `sym; `trade];  / enumerates syms, sorts by sym, applies p#
  delete from `trade;
  -1"saved partition ",string[dt]," to ",1_string HDB;
  0 };

/ Convenience: persist whatever is buffered under today's date (manual flush).
flush:{savedown[.z.d]};

-1"nano_tick schema loaded (RDB/writer). HDB=",(1_string HDB),
  " | insertRaw[rows] to load, savedown[date] to persist.";
