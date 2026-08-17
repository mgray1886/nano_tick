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
/ OHLCV+feature bars for one symbol/day:  bars[`BTCUSDT; 2026.08.13; 0D00:01]
bars:{[s;d;sz] barsOf[select time,price,qty,buyerMaker from trade where date=d, sym=s; sz] }

/ Whole-day VWAP for a symbol. Aggregate with `select` (map-reduces over
/ partitions), then pull the scalar — `exec agg from <partitioned>` is 'nyi.
vwapDay:{[s;d] first exec vwap from select vwap:qty wavg price from trade where date=d, sym=s }

/ Trades per day for a symbol — quick data-coverage / sanity check.
counts:{[s] select trades:count i by date from trade where sym=s }

-1"nano_tick analytics loaded: barsOf[t;sz], bars[sym;date;sz], vwapDay[sym;date], counts[sym]";
