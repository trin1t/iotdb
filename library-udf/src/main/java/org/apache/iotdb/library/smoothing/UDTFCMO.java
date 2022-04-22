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
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/** Double Exponential Moving Average */
public class UDTFCMO implements UDTF {

  private int window = 0;
  private int n = 0;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            window -> (int) window >= 1,
            "\"window\" should be an integer greater than one.",
            validator.getParameters().getInt("window"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    window = parameters.getInt("window");
    dataType = parameters.getDataType(0);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    n = rowWindow.windowSize();
    if (window < n) {
      double sum_up = 0, sum_down = 0, tmp, res;
      Row row, next_row;
      int i;
      for (i = 0; i < window; i++) {
        row = rowWindow.getRow(i);
        next_row = rowWindow.getRow(i + 1);
        tmp = Util.getValueAsDouble(next_row, 1) - Util.getValueAsDouble(row, 1);
        if (tmp > 0) {
          sum_up += tmp;
        } else {
          sum_down += -tmp;
        }
        Util.putValue(collector, dataType, row.getTime(), Double.NaN);
      }
      for (i = window; i < n - 1; i++) {
        row = rowWindow.getRow(i);
        next_row = rowWindow.getRow(i + 1);
        tmp = Util.getValueAsDouble(next_row, 1) - Util.getValueAsDouble(row, 1);
        if (tmp > 0) {
          sum_up += tmp;
        } else {
          sum_down += -tmp;
        }
        res = (sum_up - sum_down) * 100 / (sum_up + sum_down);
        Util.putValue(collector, dataType, row.getTime(), res);
        row = rowWindow.getRow(i - window);
        next_row = rowWindow.getRow(i + 1 - window);
        tmp = Util.getValueAsDouble(next_row, 1) - Util.getValueAsDouble(row, 1);
        if (tmp > 0) {
          sum_up -= tmp;
        } else {
          sum_down -= -tmp;
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (n == 0) {
      collector.putDouble(0, Double.NaN);
    }
  }
}
