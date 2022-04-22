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

import java.util.HashMap;

/** RANK */
public class UDTFRANK implements UDTF {

  private TSDataType dataType;
  public static HashMap<Double, Long> doubleDic;
  private DoubleArrayList doubleArrayList;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);

    dataType = parameters.getDataType(0);
    doubleDic = new HashMap<>();
    doubleArrayList = new DoubleArrayList();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double vd = row.getDouble(0);
    if (Double.isFinite(vd)) {
      doubleArrayList.add(vd);
    }
    doubleDic.put(row.getDouble(0), row.getTime());
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    doubleArrayList.sortThis();
    for (int i = 0; i < doubleArrayList.size(); i++) {
      double value = doubleArrayList.get(i);
      long time = doubleDic.getOrDefault(value, 0L);
      collector.putDouble(time, i * 1.0 / (doubleArrayList.size() - 1));
    }
  }
}
