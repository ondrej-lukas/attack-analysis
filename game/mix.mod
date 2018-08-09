set P;
param L;
param rat;
param s{i in P};
param p{i in P};
var u;
var h{i in P} >= 0.0001;
var tot;

#maximize reward: u;
minimize reward: u;

s.t. BR{i in P}:  u >= rat * s[i]/h[i] + (1-rat) * sum{j in P} (p[j]/100*s[j]/h[j]);
s.t. Prb: sum{i in P} h[i] <= L;
s.t. capacity{i in P}: h[i] <= 1 - s[i];
s.t. total: sum{i in P} h[i] = tot;
