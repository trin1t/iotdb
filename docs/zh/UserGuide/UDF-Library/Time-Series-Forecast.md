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

# 时间序列预测
## Decompose

## STL

### 函数简介

本函数使用STL方法将输入时间序列分解为趋势项、季节项、残差项之和。

**函数名：** STL

**输入序列：** 仅支持单个输入序列，类型为 DOUBLE。

**参数：**

+ `period`: 输入序列的周期长度，应当为大于 1 的整数。按数据点的个数计。
+ `swindow`: 季节项提取时的 LOESS 窗口长度（含滞后项），应当为奇数且大于等于 7. 缺省时使用周期回归。
+ `sdegree`: 季节项提取时局部拟合的多项式次数。应当为 0 或 1. 默认为 0.
+ `sjump`: 大于等于 1 的整数，用于提升季节平滑的计算速度。每隔`sjump`个点进行线性插值。默认值为 `ceil(swindow/10)`.
+ `twindow`: 趋势项提取时的 LOESS 窗口长度（含滞后项），应当为奇数。默认值为 `nextodd(ceil((1.5*period) / (1-(1.5/swindow))))`.
+ `tdegree`: 趋势项提取时局部拟合的多项式次数。应当为 0 或 1. 默认为 1.
+ `tjump`:  大于等于 1 的整数，用于提升趋势平滑的计算速度。每隔`tjump`个点进行线性插值。默认值为`ceil(twindow/10)`.
+ `lwindow`: 对每个子序列进行低通滤波时的 LOESS 窗口长度（含滞后项）。默认为不小于`period`的最小奇数.
+ `ldegree`: 子序列低通滤波时局部拟合的多项式次数。应当为 0 或 1. 默认等于`tdegree`.
+ `ljump`: 大于等于 1 的整数，用于提升低通滤波平滑的计算速度。每隔`ljump`个点进行线性插值。默认值为 `ceil(lwindow/10)`.
+ `robust`: 布尔值，指示是否在 LOESS 过程中使用鲁棒拟合。默认为 false.
+ `output`: 指示输出序列。可以取 "trend", "seasonal" 或 "residual". 默认值为 "trend".

**输出序列：** 输出单个序列，类型为 DOUBLE。

**提示：** 函数忽略输入序列中的 NaN. 认为数据点是等间隔的。用户可以先进行重采样再进行分解。

### 使用示例

#### 指定周期长度分解

输入序列：

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

用于查询的 SQL 语句：

```sql
select stl(s1, 'period' = '12', 'swindow' = '35', 'output' = 'trend') from root.test.d1
```

输出序列：

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

用于查询的 SQL 语句：

```sql
select stl(s1, 'period' = '12', 'swindow' = '35', 'output' = 'residual') from root.test.d1
```

输出序列：

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
