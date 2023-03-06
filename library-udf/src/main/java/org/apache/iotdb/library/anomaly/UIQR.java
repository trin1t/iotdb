/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.library.anomaly;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import com.google.common.math.Quantiles;

import java.util.ArrayList;

/*
This function is used to detect anomalies based on IQR.
Stream swap require user to provide Q1 and Q3, while global swap does not.
*/
public class UIQR{
  ArrayList<Double> value = new ArrayList<>();
  ArrayList<Long> timestamp = new ArrayList<>();
  String compute = "batch";
  double q1 = 0.0d;
  double q3 = 0.0d;
  double iqr = 0.0d;

  public ArrayList<Pair<Long, Double>> getIQR(SessionDataset sds) throws Exception{
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    while(sds.hasNext()){
      RowRecord row = sds.next();
      res.addAll(transform(row));
    }

    res.addAll(terminate());

    return res;
  }

  public void beforeStart(){}

  public ArrayList<Pair<Long, Double>> transform(RowRecord row) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    if (compute.equalsIgnoreCase("stream") && q3 > q1) {
      double v = row.getFields().get(0).getDoubleV();
      if (v < q1 - 1.5 * iqr || v > q3 + 1.5 * iqr) {
        res.add(Pair.of(row.getTimestamp(),v));
      }
    } else if (compute.equalsIgnoreCase("batch")) {
      double v = row.getFields().get(0).getDoubleV();
      value.add(v);
      timestamp.add(row.getTimestamp());
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate() throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    if (compute.equalsIgnoreCase("batch")) {
      q1 = Quantiles.quartiles().index(1).compute(value);
      q3 = Quantiles.quartiles().index(3).compute(value);
      iqr = q3 - q1;
    }
    for (int i = 0; i < value.size(); i++) {
      double v = value.get(i);
      if (v < q1 - 1.5 * iqr || v > q3 + 1.5 * iqr) {
        res.add(Pair.of(timestamp.get(i), v));
      }
    }
  }
}
