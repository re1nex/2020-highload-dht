Running 3m test @ http://127.0.0.1:8080
  1 threads and 1 connections
  Thread calibration: mean lat.: 3751.191ms, rate sampling interval: 15867ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.45m    29.01s    2.12m    83.57%
    Req/Sec   639.80      0.93k    3.30k    90.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.63m 
 75.000%    1.72m 
 90.000%    1.77m 
 99.000%    2.01m 
 99.900%    2.11m 
 99.990%    2.12m 
 99.999%    2.12m 
100.000%    2.12m 

#[Mean    =    86853.786, StdDeviation   =    29010.603]
#[Max     =   127205.376, Total count    =       103162]
#[Buckets =           27, SubBuckets     =         2048]
----------------------------------------------------------
  105469 requests in 3.00m, 7.33MB read
Requests/sec:    585.92
Transfer/sec:     41.69KB

Большое Stdev получилось скорее всего из-за того, что уходит достаточно много времени на поиск актуальной версии

# cpu

![alt text](https://github.com/re1nex/2020-highload-dht/blob/hw1/profiling_info/get/cpu.png)

Почти 100% ушло на get , причем не только сервиса , но и самого dao, в основном затраты на поиск актуальных данных ( сравнение , чтение )

# alloc

 ![alt text](https://github.com/re1nex/2020-highload-dht/blob/hw1/profiling_info/get/alloc.png)

также большую часть ушло на сам get ,  причина расходов такая же