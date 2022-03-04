<!--

​    Licensed to the Apache Software Foundation (ASF) under one
​    or more contributor license agreements.  See the NOTICE file
​    distributed with this work for additional information
​    regarding copyright ownership.  The ASF licenses this file
​    to you under the Apache License, Version 2.0 (the
​    "License"); you may not use this file except in compliance
​    with the License.  You may obtain a copy of the License at
​    
​        http://www.apache.org/licenses/LICENSE-2.0
​    
​    Unless required by applicable law or agreed to in writing,
​    software distributed under the License is distributed on an
​    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
​    KIND, either express or implied.  See the License for the
​    specific language governing permissions and limitations
​    under the License.

-->

# Time Series Forecast
## Decompose
### Usage

The function decomposes original time series to the addition of trending, seasonal, and residual series with STL algorithm.

**Name:** REGEXMATCH

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `period`: Period length of given time series, which should be an integer larger than 1. Counted by number of points.
+ `swindow`: The span (in lags) of the loess window for seasonal extraction, which should be odd and at least 7. Or leave it default for periodic regression.
+ `sdegree`: Degree of locally-fitted polynomial in seasonal extraction. Should be 0 or 1. Default is 0.
+ `sjump`: Integer at least one to increase speed of the seasonal smoother. Linear interpolation happens between every sjumpth value. Default is `ceil(swindow/10)`.
+ `twindow`: The span (in lags) of the loess window for trend extraction, which should be odd. Default is `nextodd(ceil((1.5*period) / (1-(1.5/swindow))))`.
+ `tdegree`: Degree of locally-fitted polynomial in trend extraction. Should be 0 or 1. Default is 1.
+ `tjump`: Integer at least one to increase speed of the trend smoother. Linear interpolation happens between every tjumpth value. Default is `ceil(twindow/10)`.
+ `lwindow`: The span (in lags) of the loess window of the low-pass filter used for each subseries. Default is the smallest odd integer greater than or equal to `period`.
+ `ldegree`: Degree of locally-fitted polynomial for the subseries low-pass filter, which should be 0 or 1. Default is equal to `tdegree`.
+ `ljump`: Integer at least one to increase speed of the low-pass filter smoother. Linear interpolation happens between every ljumpth value. Default is `ceil(lwindow/10)`.
+ `robust`: Boolean indicating if robust fitting be used in the loess procedure. Default is false.
+ `output`: Series to output. Should be "trend", "seasonal" or "residual". Default is "trend".

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Examples

#### Query with period

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.000+08:00|         315.71|
|1970-01-01T08:00:00.001+08:00|         317.45|
|1970-01-01T08:00:00.002+08:00|          317.5|
|1970-01-01T08:00:00.003+08:00|          317.1|
|1970-01-01T08:00:00.004+08:00|         315.86|
|1970-01-01T08:00:00.005+08:00|         314.93|
|1970-01-01T08:00:00.006+08:00|          313.2|
...
Total line number = 708
```

SQL for query:

```sql
select stl(s1, 'period' = '12', 'swindow' = '35', 'output' = 'trend') from root.test.d1
```

Output series:

```
+-----------------------------+---------------------------------------------------------------------+
|                         Time|stl(root.test.d1.s1, "period"="12", "swindow"="35", "output"="trend")|
+-----------------------------+---------------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                    314.8471139496001|
|1970-01-01T08:00:00.001+08:00|                                                   314.94118165200905|
|1970-01-01T08:00:00.002+08:00|                                                   315.03524935441806|
|1970-01-01T08:00:00.003+08:00|                                                    315.1204665476064|
|1970-01-01T08:00:00.004+08:00|                                                   315.20568374079465|
|1970-01-01T08:00:00.005+08:00|                                                    315.2847651687474|
|1970-01-01T08:00:00.006+08:00|                                                   315.36384659670017|
...
Total line number = 708
```
SQL for query:

```sql
select stl(s1, 'period' = '12', 'swindow' = '35', 'output' = 'residual') from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------------------------+
|                         Time|stl(root.test.d1.s1, "period"="12", "swindow"="35", "output"="residual")|
+-----------------------------+------------------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                    -0.37531049196786626|
|1970-01-01T08:00:00.001+08:00|                                                     0.14743669754562916|
|1970-01-01T08:00:00.002+08:00|                                                    -0.35080487369970115|
|1970-01-01T08:00:00.003+08:00|                                                     -0.2518283153173684|
|1970-01-01T08:00:00.004+08:00|                                                    -0.19777243845459225|
|1970-01-01T08:00:00.005+08:00|                                                       0.799142917146412|
|1970-01-01T08:00:00.006+08:00|                                                      0.6671653485419711|
...
Total line number = 708
```
## STL