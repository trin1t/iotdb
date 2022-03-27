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

package org.apache.iotdb.library.forecast;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.github.signaflo.timeseries.TimeSeries;
import com.github.signaflo.timeseries.forecast.Forecast;
import com.github.signaflo.timeseries.model.arima.Arima;
import com.github.signaflo.timeseries.model.arima.ArimaOrder;

import java.util.ArrayList;

public class UDTFARIMA implements UDTF {
  private ArrayList<Double> value = new ArrayList<>();
  private ArrayList<Long> time = new ArrayList<>();
  private int p;
  private int q;
  private int d;
  private int steps;
  private String output;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            p -> (int) p >= 0,
            "\"p\" must be a non-negative integer.",
            validator.getParameters().getInt("p"))
        .validate(
            q -> (int) q >= 0,
            "\"q\" must be a non-negative integer.",
            validator.getParameters().getInt("q"))
        .validate(
            d -> (int) d >= 0,
            "\"d\" must be a non-negative integer.",
            validator.getParameters().getInt("d"))
        .validate(
            steps -> (int) steps > 0,
            "\"steps\" must be a positive integer.",
            validator.getParameters().getIntOrDefault("steps", 1))
        .validate(
            output ->
                ((String) output).equalsIgnoreCase("fittedSeries")
                    || ((String) output).equalsIgnoreCase("forecast"),
            "\"output\" should be \"fittedSeries\" or \"forecast\"",
            validator.getParameters().getStringOrDefault("output", "fittedSeries"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    value.clear();
    time.clear();
    p = parameters.getInt("p");
    q = parameters.getInt("q");
    d = parameters.getInt("d");
    steps = parameters.getIntOrDefault("steps", 1);
    output = parameters.getStringOrDefault("output", "fittedSeries");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (!Double.isNaN(v)) {
      value.add(v);
      time.add(row.getTime());
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    double[] ts = value.stream().mapToDouble(i -> i).toArray();
    TimeSeries timeSeries = TimeSeries.from(ts);
    ArimaOrder modelOrder = ArimaOrder.order(p, d, q);
    Arima model = Arima.model(timeSeries, modelOrder);
    if (output.equalsIgnoreCase("fittedSeries")) {
      double[] fittedSeries = model.fittedSeries().asArray();
      for (int i = 0; i < fittedSeries.length; i++) {
        collector.putDouble(time.get(i), fittedSeries[i]);
      }
    } else if (output.equalsIgnoreCase("forecast")) {
      Forecast forecast = model.forecast(steps);
      double[] forecast_series = forecast.pointEstimates().asArray();

      long last_timestamp = time.get(time.size() - 1);
      for (int i = 0; i < forecast_series.length; i++) {
        collector.putDouble(i + last_timestamp + 1, forecast_series[i]);
      }
    }
  }
}
