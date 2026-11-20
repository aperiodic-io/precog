
### Replication topology, 3 nodes, ack at 2 (medians of 3)

| topology | records/s | leader µs/rec | all nodes µs/rec | B/rec, all links | leader µs/rec @100k | all µs/rec @100k | p50 / p99 / p99.9 µs @100k | leader µs/rec @1M | all µs/rec @1M | p50 / p99 / p99.9 µs @1M |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| star: leader sends to both followers (today) | 5528148 | 0.156 | 0.266 | 675.4 | 0.908 | 1.694 | 858 / 2711 / 5280 | 0.184 | 0.340 | 1040 / 5852 / 20169 |
| chain: leader -> f1 -> f2, store and forward | 6094557 | 0.109 | 0.273 | 675.4 | 0.729 | 1.840 | 906 / 3389 / 8152 | 0.132 | 0.341 | 1106 / 3372 / 5937 |
| pipelined chain: f1 forwards each chunk before parsing | 6473274 | 0.106 | 0.277 | 675.4 | 0.707 | 1.905 | 900 / 2651 / 5032 | 0.127 | 0.338 | 1094 / 3183 / 5515 |

| topology | records/s | leader µs/rec sat | leader µs/rec @100k | leader µs/rec @1M | p99 µs @100k | p99 µs @1M |
|---|---:|---:|---:|---:|---:|---:|
| star: leader sends to both followers (today) | 5474444-6037907 | 0.152-0.165 | 0.860-1.082 | 0.178-0.215 | 2079-3101 | 2435-31317 |
| chain: leader -> f1 -> f2, store and forward | 6078270-7176623 | 0.097-0.114 | 0.650-0.843 | 0.124-0.171 | 1872-3565 | 2939-3922 |
| pipelined chain: f1 forwards each chunk before parsing | 6153425-6531374 | 0.104-0.116 | 0.691-0.780 | 0.127-0.169 | 2596-2984 | 3137-14378 |
