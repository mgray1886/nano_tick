/ platform/analytics_test.q — validate analytics.q aggregations
/ ============================================================================
/ Exercises barsOf on a small in-memory trade table with known values (no HDB
/ needed, single process). Run from the repo root:  q platform/analytics_test.q
/ Exit 0 = all checks passed, 1 = a check failed.
/ NB: assert takes a SINGLE list arg, called with parens — assert("desc";cond)
/ — matching schema_test.q (KDB-X mis-parses bracket calls in script context).

\l platform/analytics.q

fails:0;
assert:{[x]
  d:x 0; c:x 1;
  $[c; -1"  ok  : ",d; -2"  FAIL: ",d];
  if[not c; fails::fails+1] };

/ Four trades across two 1-minute buckets, chosen so every field is checkable.
/ buyerMaker 0101b => trades 1,3 are aggressor-buys; 2,4 are aggressor-sells.
t:([] time:2026.08.13D00:00:05 2026.08.13D00:00:35 2026.08.13D00:01:10 2026.08.13D00:01:40;
      price:100 102 101 99f;
      qty:1 2 1 3f;
      buyerMaker:0101b);
b:0!barsOf[t;0D00:01];

-1"[1] bar count";
assert("2 bars"; 2=count b);

-1"[2] bar 1 (00:00): trades 100@1 buy, 102@2 sell";
r0:b 0;
assert("open 100";        r0[`open]=100f);
assert("high 102";        r0[`high]=102f);
assert("low 100";         r0[`low]=100f);
assert("close 102";       r0[`close]=102f);
assert("vol 3";           r0[`vol]=3f);
assert("trades 2";        r0[`trades]=2);
assert("vwap 304%3";      1e-8>abs r0[`vwap]-304%3);
assert("buyVol 1";        r0[`buyVol]=1f);
assert("sellVol 2";       r0[`sellVol]=2f);
assert("imbalance -1%3";  1e-8>abs r0[`imbalance]-(-1%3));

-1"[3] bar 2 (00:01): trades 101@1 buy, 99@3 sell";
r1:b 1;
assert("open 101";        r1[`open]=101f);
assert("close 99";        r1[`close]=99f);
assert("low 99";          r1[`low]=99f);
assert("vwap 398%4";      1e-8>abs r1[`vwap]-398%4);
assert("imbalance -0.5";  1e-8>abs r1[`imbalance]-(-0.5));

/ --- 4. quoteBarsOf: features taken from the LAST quote in the bar ---
-1"[4] quoteBarsOf";
q1:([] time:2026.08.12D00:00:10 2026.08.12D00:00:50; sym:`BTCUSDT`BTCUSDT;
      venue:`binance`binance; updateID:1 2; bid:99.99 100.0; bidSize:2 3f;
      ask:100.01 100.02; askSize:2 1f);
qb:0!quoteBarsOf[q1;0D00:01];
assert("1 quote bar";  1=count qb);
qr:qb 0;
assert("mid 100.01";   1e-8>abs qr[`mid]-100.01);
assert("spread 0.02";  1e-8>abs qr[`spread]-0.02);
assert("micro 100.015";1e-8>abs qr[`micro]-100.015);  / (100*1+100.02*3)%4
assert("qimb 0.5";     1e-8>abs qr[`qimb]-0.5);        / (3-1)%4

/ --- 5. enrichTrades: as-of join to the prevailing quote ---
-1"[5] enrichTrades (as-of join)";
t1:([] time:2026.08.12D00:00:20 2026.08.12D00:00:55; sym:`BTCUSDT`BTCUSDT;
      venue:`binance`binance; tradeID:10 11; price:100.0 100.02; qty:1 1f;
      eventTime:2026.08.12D00:00:20 2026.08.12D00:00:55; buyerMaker:01b);
enr:enrichTrades[t1;q1];
assert("trade@20s -> quote@10s bid 99.99"; 1e-8>abs (exec bid from enr)[0]-99.99);
assert("trade@55s -> quote@50s bid 100.0"; 1e-8>abs (exec bid from enr)[1]-100.0);

/ --- 6. featuresOf: no-lookahead alignment + cost-aware label ---
/ 6 one-minute bars, close = mid = 100..105.  h=1, w=1 => keep bars 1..4.
-1"[6] featuresOf (no-lookahead + cost-aware label)";
n:6; tt:2026.08.12D00:00:30+0D00:01*til n; qt:0D00:00:10+tt; p:100.0+til n;
t2:([] time:tt; sym:n#`BTCUSDT; venue:n#`binance; tradeID:til n; price:p;
      qty:n#1f; eventTime:tt; buyerMaker:n#0b);
q2:([] time:qt; sym:n#`BTCUSDT; venue:n#`binance; updateID:til n; bid:p-0.005;
      bidSize:n#1f; ask:p+0.005; askSize:n#1f);
ft:featuresOf[barsOf[t2;0D00:01]; quoteBarsOf[q2;0D00:01]; 1; 1; 0.001];
assert("4 rows (drop warm-up head + unlabelled tail)"; 4=count ft);
assert("first ret uses PAST close (log 101%100)";  1e-8>abs (first exec ret from ft)-log 101%100);
assert("first fwdRet uses FUTURE mid (log 102%101)";1e-8>abs (first exec fwdRet from ft)-log 102%101);
assert("last fwdRet uses FUTURE mid (log 105%104)"; 1e-8>abs (last exec fwdRet from ft)-log 105%104);
assert("all labels +1 (up-move beats cost)";        all 1=exec label from ft);

/ --- 7. rebucketBars: coarsen stored base bars -----------------------
/ 4 one-minute bars -> 2 two-minute bars; check the aggregation is faithful.
-1"[7] rebucketBars (coarsen stored bars)";
bb:([] time:2026.08.12D00:00 2026.08.12D00:01 2026.08.12D00:02 2026.08.12D00:03;
       sym:4#`BTCUSDT;
       open:100 101 102 103f; high:101 103 104 105f; low:99 100 101 102f;
       close:101 102 103 104f; vwap:100.5 101.5 102.5 103.5;
       vol:1 2 1 2f; trades:1 1 1 1; buyVol:1 0 1 0f; sellVol:0 2 0 2f);
rb:0!rebucketBars[bb; 0D00:02];
assert("2 coarse bars";          2=count rb);
c0:rb 0;
assert("open = first";           c0[`open]=100f);
assert("close = last in bucket"; c0[`close]=102f);
assert("high = max";             c0[`high]=103f);
assert("low = min";              c0[`low]=99f);
assert("vol = sum";              c0[`vol]=3f);
assert("trades = sum";           c0[`trades]=2);
assert("vwap vol-weighted";      1e-8>abs c0[`vwap]-((100.5*1)+(101.5*2))%3);
assert("imbalance (b-s)%vol";    1e-8>abs c0[`imbalance]+1%3);

-1"";
if[fails>0; -2 (string fails)," CHECK(S) FAILED"; exit 1];
-1"ALL ANALYTICS CHECKS PASSED.";
exit 0;
