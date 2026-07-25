/ platform/schema_test.q — validate schema.q end to end (RDB + savedown)
/ ======================================================================
/ Exercises the writer path: empty-table types, insertRaw (incl. the
/ millis->timestamp conversion), and savedown (.Q.dpft writes a partition).
/ Uses a throwaway HDB under /tmp so it never touches real data.

/ Run from the repo root:  q platform/schema_test.q
/ Exit code 0 = all checks passed, 1 = a check failed.

/ NB: the check helper takes a SINGLE list arg and is called with parens —
/ assert("desc"; cond) — not brackets. KDB-X 5.0 mis-parses bracket calls
/ f["str";x] that pass a string in script context; juxtaposition apply avoids it.

TESTHDB:"/tmp/nano_tick_hdb_test";
system "rm -rf ",TESTHDB;
system "mkdir -p ",TESTHDB;

\l platform/schema.q          / defines trade, insertRaw, savedown, HDB (default)
HDB:hsym `$TESTHDB;           / redirect writes to the throwaway HDB

/ --- assertion helper ------------------------------------------------
fails:0;
assert:{[x]
  d:x 0; c:x 1;
  $[c; -1"  ok  : ",d; -2"  FAIL: ",d];
  if[not c; fails::fails+1] };

/ --- 1. empty schema has the right column types ----------------------
-1"[1] schema types";
tc:exec c!t from meta trade;                 / column -> type char
assert("8 columns";                8=count tc);
assert("time is timestamp (p)";    tc[`time]="p");
assert("eventTime is timestamp";   tc[`eventTime]="p");
assert("sym is symbol (s)";        tc[`sym]="s");
assert("venue is symbol";          tc[`venue]="s");
assert("tradeID is long (j)";      tc[`tradeID]="j");
assert("price is float (f)";       tc[`price]="f");
assert("qty is float";             tc[`qty]="f");
assert("buyerMaker is boolean (b)";tc[`buyerMaker]="b");

/ --- 2. insertRaw: raw epoch-millis in, timestamps out ---------------
-1"[2] insertRaw + millis conversion";
nowMs:1700000000000;                          / 2023-11-14 ~epoch ms
batch:([]
  time      :nowMs+0 100;                      / raw millis (as the bridge sends)
  sym       :`BTCUSDT`BTCUSDT;
  venue     :`binance`binance;
  tradeID   :1000 1001;
  price     :42000 42001f;
  qty       :0.5 0.25;
  eventTime :nowMs+1 101;
  buyerMaker:01b);
n:insertRaw batch;
assert("insertRaw returned 2";      n=2);
assert("trade has 2 rows";          2=count trade);
assert("time became timestamp";     (exec first time from trade)=ms2ts nowMs);
assert("conversion preserves order";(exec last time from trade)=ms2ts nowMs+100);

/ --- 3. savedown: .Q.dpft writes a partition, RDB clears -------------
-1"[3] savedown -> HDB partition";
d:2023.11.14;
savedown[d];
assert("RDB cleared after savedown"; 0=count trade);
part:hsym `$TESTHDB,"/2023.11.14/trade";
assert("partition dir exists";       not ()~key part);
assert("partition has price column"; `price in key part);
assert("sym enum file written";      not ()~key hsym `$TESTHDB,"/sym");

/ --- result ----------------------------------------------------------
-1"";
if[fails>0; -2 (string fails)," CHECK(S) FAILED"; exit 1];
-1"ALL SCHEMA CHECKS PASSED.";
-1"Verify HDB readback in a FRESH shell (separate process):";
-1"  q ",TESTHDB;
-1"  then:  select cnt:count i, vwap:qty wavg price by sym from trade";
exit 0;
