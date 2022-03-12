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

package org.apache.iotdb.library.ema;

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
public class UDAFDEMA implements UDTF {

  private double cvalue1 = 0;
  private double ema1 = 0.0;
  private double ema2 = 0.0;
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
    cvalue1 = 0;
    ema1 = 0;
    ema2 = 0;
    n = 0;
    dataType = parameters.getDataType(0);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    n = rowWindow.windowSize();
    if (window < n) {
      for (int i = window; i < n; i++)
      {
        ema1=0;
        ema2=0;
        Row row = rowWindow.getRow(i);
        for (int j=0;j<window;j++)
        {
          Row row2 = rowWindow.getRow((int) i-window+j);
          cvalue1=Util.getValueAsDouble(row2, 1);
          ema1=(2.0/(window+1))*ema1+(1-2.0/(window+1))*cvalue1;
          ema2=(2.0/(window+1))*ema2+(1-2.0/(window+1))*ema1;
        }
        Util.putValue(collector, dataType, row.getTime(), 2*ema1-ema2);
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
