/ platform/analytics_test.q — validate analytics.q aggregations
/ ============================================================================
/ Exercises barsOf on a small in-memory trade table with known values (no HDB
/ needed, single process). Run from the repo root:  q platform/analytics_test.q
/ Exit 0 = all checks passed, 1 = a check failed.
/
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

-1"";
if[fails>0; -2 (string fails)," CHECK(S) FAILED"; exit 1];
-1"ALL ANALYTICS CHECKS PASSED.";
exit 0;
