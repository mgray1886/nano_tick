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

/ Best bid/ask (@bookTicker). Spot bookTicker carries NO exchange timestamp,
/ so `time` is the 3A+ receive time (recv_ts), not an exchange time.
quote:([]
  time    :`timestamp$();  / recv_ts — ingest receive time
  sym     :`symbol$();
  venue   :`symbol$();
  updateID:`long$();       / bookTicker `u`; monotonic per sym (dedup key)
  bid     :`float$();
  bidSize :`float$();
  ask     :`float$();
  askSize :`float$());

/ Precomputed OHLCV + order-flow bars, materialised per COMPLETED day at savedown
/ (option c): the reader then reads ~1440 rows/day instead of scanning millions of
/ trades. TODAY stays fast too — it is aggregated on the fly from the in-memory
/ `trade` RDB. One base granularity (BAR_SIZE = 1 min); coarser bars are
/ re-bucketed on read (analytics.q rebucketBars). Columns match analytics.q barsOf.
BAR_SIZE:0D00:01;
bar:([]
  time     :`timestamp$();  / bar-open time (BAR_SIZE bucket)
  sym      :`symbol$();
  open     :`float$();
  high     :`float$();
  low      :`float$();
  close    :`float$();
  vwap     :`float$();
  vol      :`float$();
  trades   :`long$();
  buyVol   :`float$();
  sellVol  :`float$();
  imbalance:`float$());

/ Precomputed quote bars: the LAST bid/ask/size in each BAR_SIZE bucket (the
/ point-in-time book at bar close), materialised like `bar` so featureTable's
/ quote side reads ~1440 rows instead of scanning every quote. Only the raw
/ last-in-bar values are stored; mid/spread/micro/qimb are derived on read
/ (analytics.q rebucketQuoteBars) — rebucketing is EXACT here (last of last = last).
quoteBar:([]
  time    :`timestamp$();
  sym     :`symbol$();
  bid     :`float$();
  ask     :`float$();
  bidSize :`float$();
  askSize :`float$());

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

/ Same, for quotes: only `time` (recv_ts) needs converting.
insertRawQuote:{[rows]
  `quote insert update time:ms2ts time from rows;
  count rows };

/ Tickerplant-style hook, so migrating to kdb+tick later needs no bridge change.
upd:{[t;x] t insert x};

/ Restart-safe per-symbol max id (read side). Trade/quote ids are per-symbol
/ sequences, so a column's global max isn't a symbol's max. Read the newest
/ partition's id + sym column files directly, filtered by `s`: loads the sym enum
/ domain, no `\l` (which would clash with the in-memory RDB table of the same
/ name). -1 when `s` has no rows in that partition. `hdbDir`/`p`/`tbl`/`col` are
/ strings; `s` a symbol.
partMaxId:{[hdbDir;p;tbl;col;s]
  sym::get hsym `$hdbDir,"/sym";
  ids:get hsym `$p,"/",tbl,"/",col;
  syms:get hsym `$p,"/",tbl,"/sym";
  m:ids where syms=s;
  $[count m; max m; -1] };

/ --- persistence -----------------------------------------------------
/ Append one in-memory table `t` to the HDB as partition `dt` (a date):
/ splayed, time-then-sym sorted, with the `p#` attribute on sym (all via
/ .Q.dpft), then clear it. No-op if the table is empty.
saveTable:{[dt;t]
  if[0=count value t; :0];
  `time xasc t;                / time order; .Q.dpft's sym sort is stable
  .Q.dpft[HDB; dt; `sym; t];   / enumerates syms, sorts by sym, applies p#
  delete from t;
  -1"saved ",string[t]," partition ",string[dt]," to ",1_string HDB;
  0 };

/ Aggregate a trade-shaped table into OHLCV+flow bars of width `sz`, grouped by
/ sym (columns match analytics.q barsOf). Unkeyed, `time`sym leading so .Q.dpft
/ can partition it.
buildBars:{[t;sz]
  b:select open:first price, high:max price, low:min price, close:last price,
           vwap:qty wavg price, vol:sum qty, trades:count i,
           buyVol:sum qty*not buyerMaker, sellVol:sum qty*buyerMaker,
           imbalance:%[sum qty*(1 - 2*buyerMaker); sum qty]
      by sym, time:sz xbar time from t;
  `time`sym`open`high`low`close`vwap`vol`trades`buyVol`sellVol`imbalance xcols 0!b };

/ Aggregate quotes into per-bar LAST bid/ask/size (the book at bar close), grouped
/ by sym — the raw values analytics.q's quoteBarsOf takes `last` of; derived
/ features (mid/spread/micro/qimb) are recomputed on read.
buildQuoteBars:{[q;sz]
  b:select bid:last bid, ask:last ask, bidSize:last bidSize, askSize:last askSize
      by sym, time:sz xbar time from q;
  `time`sym`bid`ask`bidSize`askSize xcols 0!b };

/ Persist precomputed bars for day `dt` (built from `trade` BEFORE saveTable
/ cleared it). No-op when the day had no trades.
saveBars:{[dt;b]
  if[0=count b; :0];
  bar::b;                      / .Q.dpft partitions the global `bar` (sorts by sym, p#)
  .Q.dpft[HDB; dt; `sym; `bar];
  delete from `bar;
  -1"saved bar partition ",string[dt]," to ",1_string HDB;
  0 };

/ Persist precomputed quote bars for day `dt` (built from `quote` before it was
/ cleared). No-op when the day had no quotes.
saveQuoteBars:{[dt;b]
  if[0=count b; :0];
  quoteBar::b;
  .Q.dpft[HDB; dt; `sym; `quoteBar];
  delete from `quoteBar;
  -1"saved quoteBar partition ",string[dt]," to ",1_string HDB;
  0 };

/ Persist trade + quote for the COMPLETED day AND its materialised bars, then
/ clear the RDB. Bars are built from `trade` first, before saveTable clears it.
/ Call at date rollover.
savedown:{[dt]
  b:buildBars[trade; BAR_SIZE];
  qb:buildQuoteBars[quote; BAR_SIZE];
  saveTable[dt;`trade];
  saveTable[dt;`quote];
  saveBars[dt; b];
  saveQuoteBars[dt; qb];
  0 };

/ Convenience: persist whatever is buffered under today's date (manual flush).
flush:{savedown[.z.d]};

-1"nano_tick schema loaded (RDB/writer). HDB=",(1_string HDB),
  " | insertRaw/insertRawQuote to load, savedown[date] to persist.";
