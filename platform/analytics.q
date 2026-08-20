/ platform/analytics.q — nano_tick feature queries over the trade HDB (reader)
/ ==========================================================================
/ Query/aggregation functions for the price-prediction work. Load in a q
/ session that already has the partitioned `trade` HDB mapped:
/     q <hdb-path>            / e.g. q ~/nano_tick_hdb
/     \l platform/analytics.q
/ or call the functions from Python via pykx. This is the READER side; the
/ writer is schema.q. Query the HDB from a SEPARATE process from the writer
/ (a table name is in-memory OR partitioned, not both).

/ --- core aggregation (works on any trade-shaped table) -------------------
/ Resample trades into OHLCV bars of width `sz` (a timespan, e.g. 0D00:01 for
/ one minute), with VWAP, trade count, buy/sell volume, and normalised
/ order-flow imbalance in [-1,1].
/ Binance `buyerMaker`=1b means the buyer was the maker, i.e. the aggressor
/ SOLD — so `not buyerMaker` marks aggressor-buy volume, and (1-2*buyerMaker)
/ signs each trade +1 (buy) / -1 (sell).
barsOf:{[t;sz]
  select open:first price, high:max price, low:min price, close:last price,
         vwap:qty wavg price, vol:sum qty, trades:count i,
         buyVol:sum qty*not buyerMaker, sellVol:sum qty*buyerMaker,
         imbalance:%[sum qty*(1 - 2*buyerMaker); sum qty]
  by bar:sz xbar time
  from t }

/ --- HDB entry points (partition-pruned by date) --------------------------
/ Re-aggregate stored base bars into width `sz` (same columns as barsOf): 1:1 at
/ the stored granularity, coarser sz merges buckets. vwap is vol-weighted (each
/ stored bar's vwap*vol is that bucket's price*qty sum), imbalance recomputed.
rebucketBars:{[b;sz]
  select open:first open, high:max high, low:min low, close:last close,
         vwap:vol wavg vwap, vol:sum vol, trades:sum trades,
         buyVol:sum buyVol, sellVol:sum sellVol,
         imbalance:%[(sum buyVol)-sum sellVol; sum vol]
    by bar:sz xbar time from b }

barsFromTrades:{[s;d;sz] barsOf[select time,price,qty,buyerMaker from trade where date=d, sym=s; sz] }

/ OHLCV+feature bars for one symbol/day:  bars[`BTCUSDT; 2026.08.13; 0D00:01].
/ Prefers the materialised `bar` table (schema.q savedown builds it, ~1440
/ rows/day, re-bucketed to sz); falls back to scanning `trade` when the HDB
/ predates bar materialisation or a day is present only as trades.
bars:{[s;d;sz]
  $[`bar in tables[];
    $[0<count b:select from bar where date=d, sym=s; rebucketBars[b; sz]; barsFromTrades[s;d;sz]];
    barsFromTrades[s;d;sz]] }

/ Whole-day VWAP for a symbol. Aggregate with `select` (map-reduces over
/ partitions), then pull the scalar — `exec agg from <partitioned>` is 'nyi.
vwapDay:{[s;d] first exec vwap from select vwap:qty wavg price from trade where date=d, sym=s }

/ Trades per day for a symbol — quick data-coverage / sanity check.
counts:{[s] select trades:count i by date from trade where sym=s }

/ --- quotes -------------------------------------------------------------
/ Best-bid/ask features per bar, taken from the LAST quote in each bar (the
/ point-in-time book state at bar close — what you'd act on):
/   mid    = (bid+ask)/2                spread = ask-bid
/   micro  = (bid*askSize + ask*bidSize) / (bidSize+askSize)   (size-weighted;
/            leans toward the heavier side, where price is likelier to move)
/   qimb   = (bidSize-askSize) / (bidSize+askSize)   in [-1,1]  (quote imbalance)
quoteBarsOf:{[q;sz]
  b: select bid:last bid, ask:last ask, bidSize:last bidSize, askSize:last askSize
     by bar: sz xbar time from q;
  update mid: 0.5*bid+ask, spread: ask-bid,
         micro: ((bid*askSize)+(ask*bidSize)) % (bidSize+askSize),
         qimb: (bidSize-askSize) % (bidSize+askSize)
    from b }

quoteBars:{[s;d;sz]
  quoteBarsOf[select time,bid,ask,bidSize,askSize from quote where date=d, sym=s; sz] }

/ --- as-of join (the kdb showcase) --------------------------------------
/ Enrich each trade with the PREVAILING (nearest-earlier) quote. aj matches
/ on sym then the last quote time <= the trade time; both inputs must be
/ time-sorted within sym (HDB partitions are). Enables effective-spread /
/ trade-sign features at trade granularity.
enrichTrades:{[t;q] aj[`sym`time; t; q] }

/ --- features + cost-aware label ----------------------------------------
/ Combine trade bars (OHLCV+imbalance) with quote bars and add trailing
/ features plus a cost-aware forward-return label.
/   h    : label horizon (bars)     w : trailing window for rolling features
/   cost : round-trip cost as a return (e.g. 0.002 = Binance taker round trip)
/ NO LOOKAHEAD: every feature uses bars <= i (prev / rolling are trailing);
/ the label (fwdRet over the NEXT h bars) is the only forward-looking column.
/ The warm-up head (w bars) and the unlabelled tail (h bars) are dropped.
featuresOf:{[tb;qb;h;w;cost]
  b: 0! tb lj qb;
  m: b`mid;
  b: update ret: log close % prev close from b;                / trailing 1-bar log return
  b: update rvol: w mdev ret, imbAvg: w mavg imbalance from b;  / trailing vol, mean flow
  b: update fwdRet: log ((h _ m),h#0n) % m from b;             / FORWARD h-bar mid return
  b: update label: (fwdRet > cost) - fwdRet < neg cost from b;  / +1/0/-1, net of cost
  (w; ((count b) - w) - h) sublist b };   / drop warm-up head (w) + unlabelled tail (h)

/ HDB entry point: labelled feature table for one symbol/day.
featureTable:{[s;d;sz;h;w;cost] featuresOf[bars[s;d;sz]; quoteBars[s;d;sz]; h; w; cost] }

-1"nano_tick analytics loaded: bars/quoteBars/enrichTrades/featureTable + barsOf/quoteBarsOf/featuresOf";
