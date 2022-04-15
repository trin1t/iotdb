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

package org.apache.iotdb.library.smoothing;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

import java.util.ArrayList;
import java.util.HashMap;

/** WINSORIZE */
public class UDTFWINSORIZE implements UDTF {

  private double l1;
  private double l2;
  private TSDataType dataType;
  public static HashMap<Long, Double> doubleDic;
  private DoubleArrayList doubleArrayList;
  private ArrayList<Long> timeArrayList;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            l1 -> (int) l1 >= 0,
            "\"window\" should be greater than zero.",
            validator.getParameters().getInt("l1"))
        .validate(
            l2 -> (int) l2 <= 1,
            "\"window\" should be smaller than one.",
            validator.getParameters().getInt("l2"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);

    l1 = parameters.getInt("l1");
    l2 = parameters.getInt("l2");
    dataType = parameters.getDataType(0);
    doubleDic = new HashMap<>();
    doubleArrayList = new DoubleArrayList();
    timeArrayList = new ArrayList<>();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double vd = row.getDouble(0);
    long time = row.getTime();
    if (Double.isFinite(vd)) {
      timeArrayList.add(time);
      doubleArrayList.add(vd);
    }
    doubleDic.put(row.getTime(), row.getDouble(0));
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    doubleArrayList.sortThis();
    double limit1 = doubleArrayList.get((int) (doubleArrayList.size() * l1));
    double limit2 = doubleArrayList.get((int) (doubleArrayList.size() * l2));
    for (int i = 0; i < doubleArrayList.size(); i++) {
      long time = timeArrayList.get(i);
      double value = doubleDic.getOrDefault(time, 0D);
      if (value < limit1) {
        collector.putDouble(time, limit1);
      } else if (value > limit2) {
        collector.putDouble(time, limit2);
      } else {
        collector.putDouble(time, doubleArrayList.get(i));
      }
    }
  }
}
