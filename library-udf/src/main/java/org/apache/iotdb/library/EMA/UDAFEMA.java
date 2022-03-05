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

package org.apache.iotdb.library.EMA;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/** Exponential Moving Average */
public class UDTFSTL implements UDTF {
  private ArrayList<Long> timestamp = new ArrayList<>();
  private ArrayList<Double> value = new ArrayList<>();

  private double value = 0;
  private double ema = 0.0;
  private long count = 0;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
            .validateInputSeriesNumber(1)
            .validateInputSeriesDataType(
                    0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
            .validate(
                    period -> (int) period > 1,
                    "\"period\" should be an integer greater than one.",
                    validator.getParameters().getInt("period"))
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
          throws Exception {
    configurations
            .setAccessStrategy(new RowByRowAccessStrategy())
            .setOutputDataType(TSDataType.DOUBLE);
    period = parameters.getInt("period");
    value = 0;
    ema = 0;
    count = 0;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0) || row.isNull(1)) {
      return;
    }
    value=Util.getValueAsDouble(row, 1);
    ema=(2.0/(period+1))*ema+(1-2.0/(period+1))*value;
    count+=1;
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (count > 0) {
      collector.putDouble(0, ema);
    } else {
      collector.putDouble(0, Double.NaN);
    }
  }
}
