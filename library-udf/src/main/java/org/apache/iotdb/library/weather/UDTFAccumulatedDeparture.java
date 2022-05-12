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

package org.apache.iotdb.library.weather;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/** Acumulated Departure */
public class UDTFAccumulatedDeparture implements UDTF {

  private double sum = 0;
  private double num = 0;
  private double avg = 0;
  private double all_avg = 0;
  private double all_sum = 0;
  private double all_num = 0;
  private int n = 0;
  private TSDataType dataType;

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
    sum = 0;
    all_sum = 0;
    num = 0;
    all_num = 0;
    avg = 0;
    all_avg = 0;
    n = 0;
    dataType = parameters.getDataType(0);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    n = rowWindow.windowSize();
    for (int i = 0; i < n; i++) {
      Row row = rowWindow.getRow(i);
      all_sum += row.getDouble(i);
      all_num += 1;
    }
    all_avg = all_sum / all_num;
    for (int i = 0; i < n; i++) {
      Row row = rowWindow.getRow(i);
      sum += row.getDouble(i);
      num += 1;
      avg = sum / num;
      Util.putValue(collector, dataType, row.getTime(), avg - all_avg);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (n == 0) {
      collector.putDouble(0, Double.NaN);
    }
  }
}
