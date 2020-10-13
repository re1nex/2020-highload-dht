1 threads and 1 connections
  Thread calibration: mean lat.: 1.049ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    19.85ms  174.02ms   2.44s    98.58%
    Req/Sec     2.11k     1.02k   39.89k    98.36%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.04ms
 75.000%    1.38ms
 90.000%    1.77ms
 99.000%  857.09ms
 99.900%    2.29s 
 99.990%    2.43s 
 99.999%    2.44s 
100.000%    2.45s 

#[Mean    =       19.854, StdDeviation   =      174.019]
#[Max     =     2443.264, Total count    =       339989]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  359997 requests in 3.00m, 23.00MB read
Requests/sec:   1999.99
Transfer/sec:    130.86KB

Большое Stdev получилось из-за того, что 1% реквестов больше 2s.
В это время скорее всего происходит сборка мусора.

# cpu


![alt text](https://github.com/re1nex/2020-highload-dht/blob/hw1/profiling_info/put/cpu.png)

46% занял put
8 % - отправка ответа
20 % - JIT
остальное ушло на парсинг и чтение запроса

# alloc

 ![alt text](https://github.com/re1nex/2020-highload-dht/blob/hw1/profiling_info/put/alloc.png)
51 % - put
17 % - на треды
4 % отправка ответа
остальное ушло на парсинг и чтение запроса 