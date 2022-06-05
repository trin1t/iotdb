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
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/** This function calculates the average of some value in history. */
public class UDTFHistoricalSim implements UDTF {
  private final ArrayList<Double> value = new ArrayList<>();
  private final ArrayList<Long> timestamp = new ArrayList<>();
  private final HashMap<Integer, Double> acc = new HashMap<>();
  private final HashMap<Integer, Integer> count = new HashMap<>();
  private final HashMap<Integer, Double> mean = new HashMap<>();
  private String aggr;
  private int period;
  private int current_year;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            aggr -> ((String) aggr).equalsIgnoreCase("d") || ((String) aggr).equalsIgnoreCase("m"),
            "Parameter \"aggr\" should be \"d\" or \"m\"",
            validator.getParameters().getStringOrDefault("aggr", "m"))
        .validate(
            period -> (int) period > 0,
            "Parameter \"period\" should be positive integer",
            validator.getParameters().getIntOrDefault("period", 5));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    if (aggr.equalsIgnoreCase("d")) {
      for (int m = 1; m <= 12; m++) {
        for (int d = 1; d <= 31; d++) {
          acc.put(m * 100 + d, 0d);
          count.put(m * 100 + d, 0);
          mean.put(m * 100 + d, 0d);
        }
      }
    }
    period = parameters.getIntOrDefault("period", 30);
    Calendar date = Calendar.getInstance();
    current_year = date.get(Calendar.YEAR);
    aggr = parameters.getStringOrDefault("aggr", "m");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Double v = Util.getValueAsDouble(row);
    Date date = new Date(row.getTime());
    int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
    if (aggr.equalsIgnoreCase("d")) {
      SimpleDateFormat ft = new SimpleDateFormat("yyyyMMdd");
      Integer day = Integer.parseInt(ft.format(date));
      acc.put(day, acc.get(day) + v);
      count.put(day, count.get(day) + 1);
    }
    if (current_year - year <= period) {
      value.add(v);
      timestamp.add(row.getTime());
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (aggr.equalsIgnoreCase("d")) {
      SimpleDateFormat ft = new SimpleDateFormat("MMdd");
      Date date = new Date();
      for(int y = 1; y <= period; y++){
        for (int m = 1; m <= 12; m++) {
          for (int d = 1; d <= 31; d++) {
            if (count.get(y * 10000 + m * 100 + d) > 0) {
              mean.put(y * 10000 + m * 100 + d, acc.get(m * 100 + d) / count.get(y * 10000 + m * 100 + d));
            }
          }
        }
      }
      double res_distance=0.0;
      int res_year=current_year;
      for(int y = current_year - period; y < current_year; y++){
        double sum=0.0;
        for (int m = 1; m <= 12; m++) {
          for (int d = 1; d <= 31; d++) {
            Double a=acc.get(y * 10000 + m * 100 + d);
            Double b=acc.get(current_year * 10000 + m * 100 + d);
            sum += (a - b) * (a - b);
          }
        }
        double distance = Math.sqrt(sum);
        if(distance<res_distance){
          res_distance=distance;
          res_year=y;
        }
      }
      collector.putDouble(0, res_year);
    }
  }
}
