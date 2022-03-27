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
## ARIMA

### Usage

This function fits input time series with ARIMA (Box-Jenkins) model, and forecasts the future series.

**Name:** ARIMA

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `p`: Parameter of AR (autoregressive) model. Should be an integer no less than 0.
+ `d`: Orders of difference. Should be an integer no less than 0.
+ `q`: Parameter of MA (moving average) model. Should be an integer no less than 0.
+ `steps`: Number of points to forecast.
+ `output`

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Model Explanation

ARIMA (Autoregressive Integrated Moving Average, Box-Jenkins) model is the most widely-used time series analysis model. It is composed of AR (Autoregressive), MA (Moving Average) and difference. There are 3 parameters in this model in total, and should be noted as ARIMA(d,p,q), which is the extension of ARMA(p,q) (traditional Box-Jenkins).

Here we introduce these parts of this model.

+ AR (Autoregressive) model

This model supposes that there is a linear relation between the value at $t$ and $p$ values ahead of it. Note the time series as $Y_t$, then the model is
$$
Y_t = \sum_{i=1}^{p}{\phi_i Y_{t-i}} + \phi_0 + \epsilon_t
$$
Here, $(\phi_0,\phi_1,\cdots,\phi_p)$ are the regressive coefficients of the linear model, and $\epsilon_t$ is residual.

We only need to assign $p$ to finish fitting. The accuracy of the model depends on the strength of autoregression of original series. When there is a strong autoregression, the model receives good effect. You may use ACF to evaluate the strength of autoregression.

+ MA (Moving Average) model

This model is in form of
$$
Y_t = \theta_0 + \epsilon_t + \sum_{i=1}^{q}{\theta_i \epsilon_{t-i}}
$$
Here  $\theta_0$ is mean of the whole series, $\epsilon_t$ residual at moment $t$, $(\theta_1, \theta_2,\cdots,\theta_q)$ are model parameters.

Hence, this model always use residual in a moving window with length $q$ to fit the value at next moment.

+ ARMA (Autoregressive Moving Average) model

This model is the implementation of AR model on MA model. That is to say, this model first computes moving average, and then does autoregressive model fitting. Therefore, there are two parameters $p,q$ in this model. The formula is
$$
Y_t = \phi_0 + \sum_{i=1}^{p}{\phi_i Y_{t-i}} + \theta_0 + \epsilon_t + \sum_{i=1}^{q}{\theta_i \epsilon_{t-i}}
$$
If we introduce the lag operator $L$, let  $L^i$ operates on some series to sign for $i$th lags of original series. Then the model is
$$
(1 - \sum_{i=1}^{p}{\phi_i L^i}) Y_t = (1 + \sum_{i=1}^{q}{\theta_i L^i})\epsilon_t + (\phi_0 + \theta_0)
$$

+ ARIMA (Autoregressive Integrated Moving Average) model

For unstationary time series, we usually compute $d$ order difference to make it stationary. Then we apply ARMA model on $d$ order difference series to get ARIMA model. ARIMA(p,d,q) has a formula
$$
(1-\sum_{i=1}^{p}{\phi_i L^i})(1-L)^d Y_t = (1+\sum_{i=1}^{q}{\theta_i L^i})\epsilon_t + (\phi_0 + \theta_0)
$$
The only difference from ARMA model is that the original series $Y_t$ is replaced by $d$ order difference $(1-L)^d Y_t$.

There are some special cases when parameters are in low order. Here are some examples.

+ ARIMA(0,1,0)	Namely MA(1). Random walk.
+ ARIMA(1,0,0)	Namely AR(1). The series has a exponential trend.
+ ARIMA(0,1,1)	Simple exponential smoothing. If there is a constant in the model, it comes to simple exponential smoothing with growth.
+ ARIMA(0,2,1)	Linear exponential smoothing.

You may choose the parameters with the following steps.

+ Compute $1$ to $d$ order difference, until the series becomes stationary. Usually you can use ADF (TBD) or QLB to conduct hypothesis test.
+ Compute ACF and PACF of stationary series. For AR(p) model, PACF truncates at $p$ lags. For MA(q) model, ACF truncates at $q$ lags. For normal situations, you should refer to AIC, BIC or other citerias.
  + Truncate: the continuing values shrinking to zero.

### Examples

Input series:

```
+-----------------------------+-------------------------+
|                         Time|root.udf.arima_test.value|
+-----------------------------+-------------------------+
|1970-01-01T08:00:00.001+08:00|                   6.7144|
|1970-01-01T08:00:00.002+08:00|                  13.3132|
|1970-01-01T08:00:00.003+08:00|                  16.1587|
|1970-01-01T08:00:00.004+08:00|                  17.3838|
|1970-01-01T08:00:00.005+08:00|                  16.0977|
|1970-01-01T08:00:00.006+08:00|                  13.8512|
|1970-01-01T08:00:00.007+08:00|                  10.5938|
|1970-01-01T08:00:00.008+08:00|                  11.2193|
|1970-01-01T08:00:00.009+08:00|                    7.058|
|1970-01-01T08:00:00.010+08:00|                   8.5565|
|1970-01-01T08:00:00.011+08:00|                  13.8039|
|1970-01-01T08:00:00.012+08:00|                  20.1125|
...
Total line number = 100
```

#### Output forecast results

SQL for query:

```sql
select arima(value, 'output' = 'forecast', 'p' = '5', 'q' = '2', 'd' = '1', 'steps' = '10') from root.udf.arima_test
```

Output series:

```
+-----------------------------+----------------------------------------------------------------------------------------------+
|Time                         |arima(root.udf.arima_test.value, "output"="forecast", "p"="5", "q"="2", "d"="1", "steps"="10")|
+-----------------------------+----------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.101+08:00|                                       																			204.9756001423199|
|1970-01-01T08:00:00.102+08:00|                                      																			 211.23778648605932|
|1970-01-01T08:00:00.103+08:00|                                      																			 215.67569955190612|
|1970-01-01T08:00:00.104+08:00|                                       																			218.4383755838573|
|1970-01-01T08:00:00.105+08:00|                                      																			 219.24618497706672|
|1970-01-01T08:00:00.106+08:00|                                      																			 218.79502918172784|
|1970-01-01T08:00:00.107+08:00|                                      																			 217.74178919857764|
|1970-01-01T08:00:00.108+08:00|                                      																			 217.02613122256972|
|1970-01-01T08:00:00.109+08:00|                                      																			 217.35635728236403|
|1970-01-01T08:00:00.110+08:00|                                      																			 219.21796483229522|
+-----------------------------+----------------------------------------------------------------------------------------------+
```

#### Output fitted series

SQL for query:

```sql
select arima(value, 'output' = 'fittedSeries', 'p' = '5', 'q' = '2', 'd' = '1') from root.udf.arima_test
```

Output series:

```
+-----------------------------+--------------------------------------------------------------------------------+
|                         Time|arima(root.udf.arima_test.value,"output"="fittedSeries","p"="5","q"="2","d"="1")|
+-----------------------------+--------------------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|                                                               6.707685424801995|
|1970-01-01T08:00:00.002+08:00|                                                               9.491853068538257|
|1970-01-01T08:00:00.003+08:00|                                                              15.604679034225455|
|1970-01-01T08:00:00.004+08:00|                                                              18.510260154091885|
|1970-01-01T08:00:00.005+08:00|                                                               16.12112100536489|
|1970-01-01T08:00:00.006+08:00|                                                              12.807117929381503|
|1970-01-01T08:00:00.007+08:00|                                                               9.060778175873251|
|1970-01-01T08:00:00.008+08:00|                                                               3.705390352560703|
|1970-01-01T08:00:00.009+08:00|                                                                             NaN|
|1970-01-01T08:00:00.010+08:00|                                                              13.946317091812077|
|1970-01-01T08:00:00.011+08:00|                                                              12.420875560534379|
|1970-01-01T08:00:00.012+08:00|                                                              20.617405853736607|
...
Total line number = 100
```



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
+ `steps`: Number of points to forecast. Timestamp of forecasted data is extended from original data, whose interval is arithmetic mean of original series. When `method ` is "residual" there is no additional output (because there is no residual).
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
select holtwinters(s1, 'period' = '12', 'method' = 'additive','steps'='10','output' = 'fitted') from root.test.d1
```

Output series:

```
+-----------------------------+----------------------------------------------------------------------------------------------------------+
|                         Time|holtwinters(root.test.d1.s1, "period"="12", "method"="additive", "steps"="10", "output"="fitted")|
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

## AR

### Usage

This function is used to build autoregressive model. 

**Name:** AR

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameter:**

+ `p`: Order. p must be a positive integer.
+ `steps`：The steps of forecast. Default is 1.
+ `output`: String indicating output. Should be "coefficient", "sequence", "forecast", or "residual". Default is coefficient。

**Output Series:** Output a single series. The type is DOUBLE. The output series could be:

- Autoregressive coefficients
- Sequence of fitting
- Results of forecast
- Residual

**Note:** This function ignores NaN values.

### Examples

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|1970-01-01T08:00:00.000+08:00|       0.615367|
|1970-01-01T08:00:00.001+08:00|       1.300980|
|1970-01-01T08:00:00.002+08:00|      -0.218203|
|1970-01-01T08:00:00.003+08:00|      -0.976345|
|1970-01-01T08:00:00.004+08:00|      -0.531409|
|1970-01-01T08:00:00.005+08:00|       0.044977|
|1970-01-01T08:00:00.006+08:00|       1.048613|
...
Total line number = 300
```

#### Output the autoregressive coefficients

SQL for query:

```sql
select ar(s1, 'p' = '5', 'output' = 'coefficient') from root.test.d1
```

Output series:

```
+-----------------------------+-------------------------------------------+
|                         Time|ar(s1, 'p' = '5', 'output' = 'coefficient')|
+-----------------------------+-------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                        0.25615431956708135|
|1970-01-01T08:00:00.001+08:00|                       -0.12861216153365415|
|1970-01-01T08:00:00.002+08:00|                        -0.3625302723212474|
|1970-01-01T08:00:00.003+08:00|                       -0.25106238043665424|
|1970-01-01T08:00:00.004+08:00|                        0.22001738996752368|
+-----------------------------+-------------------------------------------+
```

#### Output the sequence of fitting

SQL for query:

```sql
select ar(s1, 'p' = '5', 'output' = 'sequence') from root.test.d1
```

Output series:

```
+-----------------------------+----------------------------------------+
|                         Time|ar(s1, 'p' = '5', 'output' = 'sequence')|
+-----------------------------+----------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                0.615367|
|1970-01-01T08:00:00.001+08:00|                                 1.30098|
|1970-01-01T08:00:00.002+08:00|                                0.759738|
|1970-01-01T08:00:00.003+08:00|                               -0.218203|
|1970-01-01T08:00:00.004+08:00|                               -0.976345|
|1970-01-01T08:00:00.005+08:00|                     -0.6886951481757273|
|1970-01-01T08:00:00.006+08:00|                     0.16404891626883172|
|1970-01-01T08:00:00.007+08:00|                      0.6557594681266603|
|1970-01-01T08:00:00.008+08:00|                      0.6525890550832002|
...
Total line number = 300
```

#### Output the results of forecast

SQL for query:

```sql
select ar(s1, 'p' = '5', 'output' = 'forecast', 'steps' = '5') from root.test.d1
```

Output series:

```
+-----------------------------+-------------------------------------------------------+
|                         Time|ar(s1, 'p' = '5', 'output' = 'sequence', 'steps' = '5')|
+-----------------------------+-------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                   -0.30958964586848625|
|1970-01-01T08:00:00.001+08:00|                                     0.6191163629660078|
|1970-01-01T08:00:00.002+08:00|                                     0.9261617123785985|
|1970-01-01T08:00:00.003+08:00|                                    0.25702299037943743|
|1970-01-01T08:00:00.004+08:00|                                    -0.4136313859656312|
+-----------------------------+-------------------------------------------------------+
```

#### Output the residual

SQL for query:

```sql
select ar(s1, 'p' = '5', 'output' = 'residual') from root.test.d1
```

Output series:

```
+-----------------------------+----------------------------------------+
|                         Time|ar(s1, 'p' = '5', 'output' = 'residual')|
+-----------------------------+----------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                     0.0|
|1970-01-01T08:00:00.001+08:00|                                     0.0|
|1970-01-01T08:00:00.002+08:00|                                     0.0|
|1970-01-01T08:00:00.003+08:00|                                     0.0|
|1970-01-01T08:00:00.004+08:00|                                     0.0|
|1970-01-01T08:00:00.005+08:00|                     0.15728614817572728|
|1970-01-01T08:00:00.006+08:00|                    -0.11907191626883172|
|1970-01-01T08:00:00.007+08:00|                      0.3928535318733397|
|1970-01-01T08:00:00.008+08:00|                     0.23926594491679976|
...
Total line number = 300
```

## SARIMA

### Usage

This function fits input time series with SARIMA model, and forecasts the future series.

**Name:** SARIMA

**Input Series:** Only support a single input series. The data type is INT32 / INT64 / FLOAT / DOUBLE.

**Parameters:**

+ `p`: Parameter of AR (autoregressive) model. Should be an integer no less than 0.
+ `d`: Orders of difference. Should be an integer no less than 0.
+ `q`: Parameter of MA (moving average) model. Should be an integer no less than 0.
+ `P`: Parameter of seasonal AR model. Should be an integer no less than 0.
+ `D`: Orders of seasonal difference. Should be an integer no less than 0.
+ `Q`: Parameter of seasonal MA model. Should be an integer no less than 0.
+ `steps`: Number of points to forecast.
+ `output`

**Output Series:** Output a single series. The type is DOUBLE.

**Note:** This function ignores NaN values. Data points are considered with equal time interval. User may resample first before doing decomposition.

### Examples

Input series

```
+-----------------------------+--------------------------+
|                         Time|root.udf.sarima_test.value|
+-----------------------------+--------------------------+
|2000-01-01T08:00:00.001+08:00|                     76273|
|2000-02-01T08:00:00.001+08:00|                     70994|
|2000-03-01T08:00:00.001+08:00|                     72795|
|2000-04-01T08:00:00.001+08:00|                     79561|
|2000-05-01T08:00:00.001+08:00|                     92584|
|2000-06-01T08:00:00.001+08:00|                    102644|
|2000-07-01T08:00:00.001+08:00|                    111101|
|2000-08-01T08:00:00.001+08:00|                    115467|
|2000-09-01T08:00:00.001+08:00|                    108515|
|2000-10-01T08:00:00.001+08:00|                    105465|
|2000-11-01T08:00:00.001+08:00|                     95800|
|2000-12-01T08:00:00.001+08:00|                     89610|
|2001-01-01T08:00:00.001+08:00|                     82219|
|2001-02-01T08:00:00.001+08:00|                     74830|
|2001-03-01T08:00:00.001+08:00|                     79845|
...
Total line number = 229
```

#### Output forecast results

sql for query

```sql
select sarima(value, 'output' = 'forecast', 'p' = '4', 'q' = '1', 'd' = '1', 'P' = '2', 'Q' = '1', 'D' = '1', 'steps' = '20') from root.udf.sarima_test
```

output series

```
+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
|                         Time|sarima(root.udf.arima_test.value, "output"="forecast", "p"="4", "q"="1", "d"="1", "P"="2", "Q"="1", "D"="1", "steps"="20")|
+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.101+08:00|                                                                                                         204.6209214376484|
|1970-01-01T08:00:00.102+08:00|                                                                                                        210.74299281324542|
|1970-01-01T08:00:00.103+08:00|                                                                                                        215.35740143689904|
|1970-01-01T08:00:00.104+08:00|                                                                                                          217.371156756569|
|1970-01-01T08:00:00.105+08:00|                                                                                                        217.25145744226663|
|1970-01-01T08:00:00.106+08:00|                                                                                                         215.3795312079138|
|1970-01-01T08:00:00.107+08:00|                                                                                                        212.85146352021266|
|1970-01-01T08:00:00.108+08:00|                                                                                                        210.56066874448348|
|1970-01-01T08:00:00.109+08:00|                                                                                                        209.67923065745984|
|1970-01-01T08:00:00.110+08:00|                                                                                                         210.9768721252065|
|1970-01-01T08:00:00.111+08:00|                                                                                                        214.51582186073625|
|1970-01-01T08:00:00.112+08:00|                                                                                                        219.96010034091134|
|1970-01-01T08:00:00.113+08:00|                                                                                                        226.45420989817816|
|1970-01-01T08:00:00.114+08:00|                                                                                                        232.91431794034597|
|1970-01-01T08:00:00.115+08:00|                                                                                                        238.23117706164606|
|1970-01-01T08:00:00.116+08:00|                                                                                                        241.59295703294964|
|1970-01-01T08:00:00.117+08:00|                                                                                                        242.69392439478455|
|1970-01-01T08:00:00.118+08:00|                                                                                                        241.75345589017832|
|1970-01-01T08:00:00.119+08:00|                                                                                                         239.5036928369731|
|1970-01-01T08:00:00.120+08:00|                                                                                                        236.99516916720017|
+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
```

#### output fitted series

sql for query

```sql
select sarima(value, 'output' = 'fittedSeries', 'p' = '4', 'q' = '1', 'd' = '1', 'P' = '2', 'Q' = '1', 'D' = '1') from root.udf.sarima_test
```

output series

```
+-----------------------------+-----------------------------------------------------------------------------------------------------------------+
|                         Time|sarima(root.udf.sarima_test.value, "output"="fittedSeries", "p"="4", "q"="1", "d"="1", "P"="2", "Q"="1", "D"="1")|
+-----------------------------+-----------------------------------------------------------------------------------------------------------------+
|2000-01-01T08:00:00.001+08:00|                                                                                                76238.88968989142|
|2000-02-01T08:00:00.001+08:00|                                                                                                71108.13399262477|
|2000-03-01T08:00:00.001+08:00|                                                                                                69090.67995088271|
|2000-04-01T08:00:00.001+08:00|                                                                                                77059.64596370702|
|2000-05-01T08:00:00.001+08:00|                                                                                                90584.17467718612|
|2000-06-01T08:00:00.001+08:00|                                                                                               104743.82111610338|
|2000-07-01T08:00:00.001+08:00|                                                                                               111689.32431887739|
|2000-08-01T08:00:00.001+08:00|                                                                                               114885.87184020969|
|2000-09-01T08:00:00.001+08:00|                                                                                               113847.66031971105|
|2000-10-01T08:00:00.001+08:00|                                                                                               100816.28616052556|
|2000-11-01T08:00:00.001+08:00|                                                                                                94384.27096121015|
|2000-12-01T08:00:00.001+08:00|                                                                                                89911.14264540777|
|2001-01-01T08:00:00.001+08:00|                                                                                                82148.07614332357|
|2001-02-01T08:00:00.001+08:00|                                                                                                78605.60378163874|
|2001-03-01T08:00:00.001+08:00|                                                                                                76442.34862369618|
|2001-04-01T08:00:00.001+08:00|                                                                                                85583.74539755435|
|2001-05-01T08:00:00.001+08:00|                                                                                               101929.42040485133|
|2001-06-01T08:00:00.001+08:00|                                                                                               114973.62891475212|
|2001-07-01T08:00:00.001+08:00|                                                                                               123878.71705603256|
|2001-08-01T08:00:00.001+08:00|                                                                                               131463.67176800632|
...
Total line number = 229
```

