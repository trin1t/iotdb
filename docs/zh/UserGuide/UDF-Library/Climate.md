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

# 气象统计


## Departure

### 函数简介

本函数用于计算原序列的距平。

**函数名：** Depature

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE。

**参数：**

+ `aggr`: 历史同期的长度，输入 "m" 以月为周期，输入 "d" 以日为周期。

**输出序列：** 输出单个序列，类型为 DOUBLE。

### 使用示例


输入序列：

```

```

用于查询的 SQL 语句：

```sql

```

输出序列：

```

```

## HistoryAvg

### 函数简介

本函数用于计算历史同期的均值。

**函数名：** HistoryAvg

**输入序列：** 仅支持单个输入序列，类型为 INT32 / INT64 / FLOAT / DOUBLE。

**参数：**

+ `aggr`: 历史同期的长度，输入 "m" 以月为周期，输入 "d" 以日为周期。

**输出序列：** 输出单个序列，类型为 DOUBLE。

### 使用示例


输入序列：

```

```

用于查询的 SQL 语句：

```sql

```

输出序列：

```

```
