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

This function decomposes input time series to the addition or multiplication of trending, seasonal, and residual series with classical additive or multiplicative model.
**Name:** Decompose

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `period`: Period length of input time series, which should be an integer larger than 1. Counted by number of points.
+ `method`: Model to fit when doing decomposition. Should be "additive" or "multiplicative". Default is "additive".
+ `output`: String indicating output. Should be "trend", "seasonal" or "residual". Default is "trend".

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Model Explanation

Classical model supposes the time series as overlay of seasonal effect on overall trend.
The seasonal effect means the periodic change of data. For example, data may fluctuate with a period of four seasons, or twelve months, and so on.
Classical model treats data as the addition or multiplication of trending term and seasonal term. And the reminder is residual.
If we note the original time series data as $$Y_t$$, trending term as $$T_t$$, seasonal term as $$S_t$$, residual as $$R_t$$,
then classical model can be written as
$$
Y_t = T_t + S_t + R_t\newline
Y_t = T_t * S_t * R_t
$$

In classical model, trending item at a certain moment is mere the moving average centering at this moment, with window length equal to the length of period.
(If the window length is an even number, then it is added by one, while the weight of data at the margin of window decreases by half.)
If you suppose to get a linear trend, please try Holt-Winters function.

Seasonal item is the arithmetical mean of data at the same phase position of a period, removing trending term.
For example, a time series has a period of four seasons. After the trending term is removed, the arithmetical mean of all data in spring is the seasonal item in spring.
Therefore, seasonal item repeats periodically.

The advantage of this model lies in its simplicity, so calculation takes very little time.
For more robustness, you may try STL function for decomposition.
The disadvantage is that there are undecomposable points at the beginning and the end of original series, which lasts half of the window length separately.
If you merely desire trending item, you may use MvAvg function in Data Profiling package instead.

### Examples

#### Decomposition with additive model

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
select decompose(s1, 'period' = '12', 'method' = 'additive', 'output' = 'trend') from root.test.d1
```

Output series:

```
+-----------------------------+--------------------------------------------------------------------------------+
|                         Time|decompose(root.test.d1.s1, "period"="12", "method"="additive", "output"="trend")|
+-----------------------------+--------------------------------------------------------------------------------+
|1970-01-01T08:00:00.006+08:00|                                                              315.40916666666664|
|1970-01-01T08:00:00.007+08:00|                                                              315.46208333333334|
|1970-01-01T08:00:00.008+08:00|                                                                       315.50625|
|1970-01-01T08:00:00.009+08:00|                                                               315.5829166666667|
|1970-01-01T08:00:00.010+08:00|                                                              315.65500000000003|
|1970-01-01T08:00:00.011+08:00|                                                               315.6779166666667|
|1970-01-01T08:00:00.012+08:00|                                                              315.69916666666666|
...
Total line number = 696
```
SQL for query:

```sql
select decompose(s1, 'period' = '12', 'method' = 'additive', 'output' = 'residual') from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------------------------------------------------+
|                         Time|decompose(root.test.d1.s1, "period"="12", "method"="additive", "output"="residual")|
+-----------------------------+-----------------------------------------------------------------------------------+
|1970-01-01T08:00:00.006+08:00|                                                                 0.9237356321838588|
|1970-01-01T08:00:00.007+08:00|                                                                0.43947557471259424|
|1970-01-01T08:00:00.008+08:00|                                                               -0.11025862068976355|
|1970-01-01T08:00:00.009+08:00|                                                              -0.028714080459828817|
|1970-01-01T08:00:00.010+08:00|                                                               -0.07654454022997274|
|1970-01-01T08:00:00.011+08:00|                                                                0.01531609195390815|
|1970-01-01T08:00:00.012+08:00|                                                                -0.4370043103448982|
...
Total line number = 696
```
#### Decomposition with multiplicative model

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
select decompose(s1, 'period' = '12', 'method' = 'multiplicative', 'output' = 'trend') from root.test.d1
```

Output series:

```
+-----------------------------+--------------------------------------------------------------------------------------+
|                         Time|decompose(root.test.d1.s1, "period"="12", "method"="multiplicative", "output"="trend")|
+-----------------------------+--------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.006+08:00|                                                                    315.40916666666664|
|1970-01-01T08:00:00.007+08:00|                                                                    315.46208333333334|
|1970-01-01T08:00:00.008+08:00|                                                                             315.50625|
|1970-01-01T08:00:00.009+08:00|                                                                     315.5829166666667|
|1970-01-01T08:00:00.010+08:00|                                                                    315.65500000000003|
|1970-01-01T08:00:00.011+08:00|                                                                     315.6779166666667|
|1970-01-01T08:00:00.012+08:00|                                                                    315.69916666666666|
...
Total line number = 696
```
SQL for query:

```sql
select decompose(s1, 'period' = '12', 'method' = 'multiplicative', 'output' = 'residual') from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------------------------------------------------------+
|                         Time|decompose(root.test.d1.s1, "period"="12", "method"="multiplicative", "output"="residual")|
+-----------------------------+-----------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.006+08:00|                                                                        1.001927194431198|
|1970-01-01T08:00:00.007+08:00|                                                                       1.0003686308901425|
|1970-01-01T08:00:00.008+08:00|                                                                         0.99899519389507|
|1970-01-01T08:00:00.009+08:00|                                                                       0.9996499295945565|
|1970-01-01T08:00:00.010+08:00|                                                                       0.9997951326019452|
|1970-01-01T08:00:00.011+08:00|                                                                       1.0002873633298288|
|1970-01-01T08:00:00.012+08:00|                                                                       0.9990986638746625|
...
Total line number = 696
```

## Holt-Winters

### Usage

This function decomposes input time series to the addition or multiplication of trending, seasonal, and residual series with Holt-Winters model, and is able to forecast continuing data.

**Name：** HoltWinters

**Input Series：** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters：**

+ `period`: Period length of input time series, which should be an integer larger than 1. Counted by number of points.
+ `method`: Model to decompose. Should be "additive", "multiplicative" or "linear". Default is "additive".
+ `alpha`: Data smoothening factor of Holt-Winters model, ranging in [0,1]. Default is 0.5.
+ `beta`: Trend smoothening factor of Holt-Winters model, ranging in [0,1]. Default is 0.5.
+ `gamma`: Seasonal smoothening factor of Holt-Winters model, ranging in [0,1]. Effective only when `method` is not "linear" . Default is 0.5.
+ `forecastNumber`: Number of points to forecast. Timestamp of forecasted data is extended from original data, whose interval is arithmetic mean of original series. When `method ` is "residual" there is no additional output (because there is no residual).
+ `auto`: Boolean. If to select model parameters automatically. If set to "true", the function will optimize RMSE of fitted model with BOBYQA, and input `alpha`, `beta`, `gamma` will be used as initial searching point. Default is "true".
+ `maxEval`: Maximum iteration times when automatically searching parameters.
+ `output`:  String indicating output. Should be "trend", "seasonal", "fitted" or "residual". Default is "fitted".

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Model Explanation

Holt-Winters model is based on exponential moving average (see EMA). To solve lagging in smoothened data, we may use double exponential moving average (see DEMA), namely `method` = "linear". Note original time series as  $$Y_t$$, trending term as $$T_t$$, seasonal term as $$S_t$$, residual term as $$R_t$$, then the model is
$$
Y_t = S_t + T_t + R_t
\newline
S_t = \alpha Y_t + (1 - \alpha)(S_{t-1}+T_{t-1})
\newline
T_t = \beta (s_t - s_{t-1}) + (1 - \beta)b_{t-1}
$$
Forecast  $$F_{t+m}$$ at the $$m$$th moment after moment $$t$$  is
$$
F_{t+m} = S_t + m * T_t
$$
If we take the variation of trending term into consideration, we may use triple exponential moving average. (see TEMA). This is useful when overall trend is linear and and period length $$L$$ is fixed. The additive model is
$$
Y_t = T_t + S_t + R_t\newline
S_0 = Y_0\newline
S_t = \alpha(Y_t-C_{t-L}) + (1 - \alpha)(S_{t-1} + T_{t-1})\newline
T_t = \beta (S_t - S_{t-1}) + (1 - \beta)T_{t-1}\newline
C_t = \gamma (Y_t - S_{t-1} - T_{t-1}) + (1 - \gamma)C_{t-L}\newline
F_{t+m} = S_t + mT_t + C_{t-L+1+(m-1) \mod L}
$$
And the multiplicative model is
$$
Y_t = T_t * S_t * R_t\newline
S_0 = Y_0\newline
S_t = \alpha\frac{Y_t}{C_{t-L}} + (1 - \alpha)(S_{t-1} + T_{t-1})\newline
T_t = \beta (S_t - S_{t-1}) + (1 - \beta)T_{t-1}\newline
C_t = \gamma \frac{Y_t}{S_t} + (1 - \gamma)C_{t-L}\newline
F_{t+m} = (S_t + mT_t)C_{t-L+1+(m-1) \mod L}
$$

Here $$C_t$$ is the linear variation speed of trending term.

### Examples

#### Decomposition with additive model and auto parameters

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
select holtwinters(s1, 'period' = '12', 'method' = 'additive','forecastNumber'='10','output' = 'fitted') from root.test.d1
```

Output series:

```
+-----------------------------+----------------------------------------------------------------------------------------------------------+
|                         Time|holtwinters(root.test.d1.s1, "period"="12", "method"="additive", "forecastNumber"="10", "output"="fitted")|
+-----------------------------+----------------------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                                                         315.7702777777778|
|1970-01-01T08:00:00.001+08:00|                                                                                         317.5401997144624|
|1970-01-01T08:00:00.002+08:00|                                                                                        317.60462474676166|
|1970-01-01T08:00:00.003+08:00|                                                                                        317.21114452436507|
|1970-01-01T08:00:00.004+08:00|                                                                                         315.9736376452674|
|1970-01-01T08:00:00.005+08:00|                                                                                         315.0440856111198|
|1970-01-01T08:00:00.006+08:00|                                                                                        313.31350064811704|
...
Total line number = 718
```

SQL for query:

```sql
select holtwinters(s1, 'period' = '12', 'method' = 'additive', 'output' = 'residual') from root.test.d1
```

Output series:

```
+-----------------------------+-------------------------------------------------------------------------------------+
|                         Time|holtwinters(root.test.d1.s1, "period"="12", "method"="additive", "output"="residual")|
+-----------------------------+-------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                                 -0.06027777777779875|
|1970-01-01T08:00:00.001+08:00|                                                                  -0.0901997144624147|
|1970-01-01T08:00:00.002+08:00|                                                                 -0.10462474676165812|
|1970-01-01T08:00:00.003+08:00|                                                                 -0.11114452436504507|
|1970-01-01T08:00:00.004+08:00|                                                                 -0.11363764526737441|
|1970-01-01T08:00:00.005+08:00|                                                                 -0.11408561111977633|
|1970-01-01T08:00:00.006+08:00|                                                                 -0.11350064811705352|
...
Total line number = 696
```

## STL

### Usage

This function decomposes input time series to the addition of trending, seasonal, and residual series with STL algorithm.

**Name:** STL

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `period`: Period length of input time series, which should be an integer larger than 1. Counted by number of points.
+ `swindow`: The span (in lags) of the loess window for seasonal extraction, which should be odd and at least 7. Or leave it default for periodic regression.
+ `sdegree`: Degree of locally-fitted polynomial in seasonal extraction. Should be 0 or 1. Default is 0.
+ `sjump`: Integer at least one to increase speed of the seasonal smoothener. Linear interpolation happens between every `sjump`th value. Default is `ceil(swindow/10)`.
+ `twindow`: The span (in lags) of the loess window for trend extraction, which should be odd. Default is `nextodd(ceil((1.5*period) / (1-(1.5/swindow))))`.
+ `tdegree`: Degree of locally-fitted polynomial in trend extraction. Should be 0 or 1. Default is 1.
+ `tjump`: Integer at least one to increase speed of the trend smoothener. Linear interpolation happens between every `tjump`th value. Default is `ceil(twindow/10)`.
+ `lwindow`: The span (in lags) of the loess window of the low-pass filter used for each subseries. Default is the smallest odd integer greater than or equal to `period`.
+ `ldegree`: Degree of locally-fitted polynomial for the subseries low-pass filter, which should be 0 or 1. Default is equal to `tdegree`.
+ `ljump`: Integer at least one to increase speed of the low-pass filter smoothener. Linear interpolation happens between every `ljump`th value. Default is `ceil(lwindow/10)`.
+ `robust`: Boolean indicating if robust fitting be used in the loess procedure. Default is false.
+ `output`: String indicating output. Should be "trend", "seasonal" or "residual". Default is "trend".

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Model Explanation
Here we only offer simple introduction. For detailed information, please refer to

Cleveland, R. B., Cleveland, W. S., McRae, J. E., & Terpenning, I. J. (1990). STL: A seasonal-trend decomposition procedure based on loess. Journal of Official Statistics, 6(1), 3–33.

STL is the abbreviate of Seasonal and Trend decomposition using Loess, which adopts additive model.
In comparison to classical model, STL utilize LOESS (locally weighted regression) to smoothen trending term.
You may refer to any tutorial book on linear regression for detailed introduction of LOESS.
The advantage over classical model is that it allows seasonal term change with time. It is also insensitive to outliers, as seen in the picture. Besides, it can decompose the beginning and the end of the series.

![stl-example](G:\iotdb\docs\UserGuide\UDF-Library\stl-example.png)

There are quite a few parameters for this function. Only `period` is a necessity. Other parameters keep same with stl in R (only "." is omitted).
You may refer to [R documentation](https://search.r-project.org/R/refmans/stats/html/stl.html).

### Examples

#### Decomposition with given period

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
