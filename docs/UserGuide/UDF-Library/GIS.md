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

# GIS

## Area

### Usage

This function is used to calculate the area of polygon with given vertices.

**Name:** AREA

**Input Series:** Only support two input numeric series. The type is FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE.

**Parameters:**

+`unit`: Aimed unit for output area. "km2", "m2", "sqmi", "sqyd", "acre" are supported.

**Note:**

+ The first input series should be latitudes, and the second input series should be longitudes.
    + Legal intervals of latitudes and longitudes are $[-180,180]$ and $[-90,90]$ separately. 
    + If there is missing value or illegal value at one point, this point will be ignored.
+ The following units are supported:
    + "km2" for square kilometer. It may be replaced by "squarekilometer".
    + "m2" for square meter. It may be replaced by "squaremeter".
    + "sqmi" for square mile. It may be replaced by "squaremile".
    + "sqyd" for square yard. It may be replaced by "squareyard".
    + "acre" for acre. It may be replaced by "ac".
+ The area of polygon is calculated on the surface of sphere, not on a projection.

### Examples

Input series:

```
+-----------------------------+----------------+-----------------+
|                         Time|root.test.d1.lat|root.test.d1.long|
+-----------------------------+----------------+-----------------+
|1970-01-01T08:00:00.000+08:00|           116.8|             39.7|
|1970-01-01T08:00:01.000+08:00|           116.5|             40.0|
|1970-01-01T08:00:02.000+08:00|           116.2|             40.1|
|1970-01-01T08:00:03.000+08:00|           115.9|             39.9|
|1970-01-01T08:00:04.000+08:00|           116.2|             39.8|
+-----------------------------+----------------+-----------------+
```

SQL for query:

```sql
select area(lat,long,'unit'='km2') from root.test.d1
```

Output series:

```
+-----------------------------+-------------------------------------------------------+
|                         Time|area(root.test.d1.lat, root.test.d1.long, "unit"="km2")|
+-----------------------------+-------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                       905.035533357631|
+-----------------------------+-------------------------------------------------------+
```

## Mileage

### Usage
****
This function is used to calculate the accumulated mileage of given trace.

**Name:** MILEAGE

**Input Series:** Only support two input numeric series. The type is FLOAT / DOUBLE.

**Output Series:** Output a single series. The type is DOUBLE.

**Parameters:**

+`unit`: Aimed unit for output length. "km", "m", "mi", "yd", "nm" are supported.

**Note:**

+ The first input series should be latitudes, and the second input series should be longitudes.
    + Legal intervals of latitudes and longitudes are $[-180,180]$ and $[-90,90]$ separately.
    + If there is missing value or illegal value at one point, this point will be ignored.
+ The following units are supported:
    + "km" for kilometer. It may be replaced by "kilometer".
    + "m" for meter. It may be replaced by "meter".
    + "mi" for mile. It may be replaced by "mile".
    + "yd" for square yard. It may be replaced by "yard".
    + "nm" for nautical mile. It may be replaced by "nmi", "nauticalmile".
+ The length of trace is calculated with great circle distance on earth surface.

### Examples

Input series:

```
+-----------------------------+----------------+-----------------+
|                         Time|root.test.d1.lat|root.test.d1.long|
+-----------------------------+----------------+-----------------+
|1970-01-01T08:00:00.000+08:00|           116.8|             39.7|
|1970-01-01T08:00:01.000+08:00|           116.5|             40.0|
|1970-01-01T08:00:02.000+08:00|           116.2|             40.1|
|1970-01-01T08:00:03.000+08:00|           115.9|             39.9|
|1970-01-01T08:00:04.000+08:00|           116.2|             39.8|
+-----------------------------+----------------+-----------------+
```

SQL for query:

```sql
select mileage(lat,long,'unit'='km') from root.test.d1
```

Output series:

```
+-----------------------------+---------------------------------------------------------+
|                         Time|mileage(root.test.d1.lat, root.test.d1.long, "unit"="km")|
+-----------------------------+---------------------------------------------------------+
|1970-01-01T08:00:00.000+08:00|                                                      0.0|
|1970-01-01T08:00:01.000+08:00|                                        42.05814079353064|
|1970-01-01T08:00:02.000+08:00|                                        69.91135507566852|
|1970-01-01T08:00:03.000+08:00|                                       103.78888718889525|
|1970-01-01T08:00:04.000+08:00|                                       131.71024653665967|
+-----------------------------+---------------------------------------------------------+
```