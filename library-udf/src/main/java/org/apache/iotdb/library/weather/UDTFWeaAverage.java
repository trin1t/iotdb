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

/** Acumulated Departure */
public class UDTFWeaAverage implements UDTF {
  private final ArrayList<Double> value = new ArrayList<>();
  private final ArrayList<Long> timestamp = new ArrayList<>();
  private final HashMap<Integer, Double> acc = new HashMap<>();
  private final HashMap<Integer, Integer> count = new HashMap<>();
  private final HashMap<Integer, Double> mean = new HashMap<>();
  private String aggr;
  private int period;
  private int current_year;
  private long start;
  private long end;
  long startTime;
  long startMemory;

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
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    if (validator.getParameters().hasAttribute("start")) {
      validator.validate(
          start -> (long) start > 0,
          "Parameter \"start\" should conform to the format yyyy-MM-dd HH:mm:ss.",
          format.parse(validator.getParameters().getString("start")).getTime());
    }
    if (validator.getParameters().hasAttribute("end")) {
      validator.validate(
          end -> (long) end > 0,
          "Parameter \"end\" should conform to the format yyyy-MM-dd HH:mm:ss.",
          format.parse(validator.getParameters().getString("end")).getTime());
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    aggr = parameters.getStringOrDefault("aggr", "m");
    if (aggr.equalsIgnoreCase("m")) {
      for (int m = 1; m <= 12; m++) {
        acc.put(m, 0d);
        count.put(m, 0);
        mean.put(m, 0d);
      }
    } else if (aggr.equalsIgnoreCase("d")) {
      for (int m = 1; m <= 12; m++) {
        for (int d = 1; d <= 31; d++) {
          acc.put(m * 100 + d, 0d);
          count.put(m * 100 + d, 0);
          mean.put(m * 100 + d, 0d);
        }
      }
    }

    period = parameters.getIntOrDefault("period", 5);

    Calendar date = Calendar.getInstance();
    current_year = date.get(Calendar.YEAR);

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    start = 0;
    if (parameters.hasAttribute("start")) {
      start = format.parse(parameters.getString("start")).getTime();
    }
    end = new Date().getTime();
    if (parameters.hasAttribute("end")) {
      end = format.parse(parameters.getString("end")).getTime();
    }

    startTime = System.currentTimeMillis(); // 获取开始时间
    startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long t = row.getTime();
    Double v = Util.getValueAsDouble(row);
    Date date = new Date(t);
    if (t > start && t < end) {
      value.add(v);
      timestamp.add(row.getTime());
    }
    int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
    if (current_year - year <= period) {
      if (aggr.equalsIgnoreCase("m")) {
        SimpleDateFormat ft = new SimpleDateFormat("MM");
        Integer month = Integer.parseInt(ft.format(date));
        acc.put(month, acc.get(month) + v);
        count.put(month, count.get(month) + 1);
      } else if (aggr.equalsIgnoreCase("d")) {
        SimpleDateFormat ft = new SimpleDateFormat("MMdd");
        Integer day = Integer.parseInt(ft.format(date));
        acc.put(day, acc.get(day) + v);
        count.put(day, count.get(day) + 1);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    double meanValue;
    if (aggr.equalsIgnoreCase("m")) {
      SimpleDateFormat ft = new SimpleDateFormat("MM");
      Date date = new Date();
      for (int m = 1; m <= 12; m++) {
        mean.put(m, acc.get(m) / count.get(m));
      }
      for (int i = 0; i < value.size(); i++) {
        meanValue = mean.get(Integer.parseInt(ft.format(date)));
        date.setTime(timestamp.get(i));
        collector.putDouble(timestamp.get(i), meanValue);
      }
    } else if (aggr.equalsIgnoreCase("d")) {
      SimpleDateFormat ft = new SimpleDateFormat("MMdd");
      Date date = new Date();
      for (int m = 1; m <= 12; m++) {
        for (int d = 1; d <= 31; d++) {
          if (count.get(m * 100 + d) > 0) {
            mean.put(m * 100 + d, acc.get(m * 100 + d) / count.get(m * 100 + d));
          }
        }
      }
      for (int i = 0; i < value.size(); i++) {
        meanValue = mean.get(Integer.parseInt(ft.format(date)));
        date.setTime(timestamp.get(i));
        collector.putDouble(timestamp.get(i), meanValue);
      }
    }
  }

  /* just for test*/
  @Override
  public void beforeDestroy() {
    long endTime = System.currentTimeMillis(); // 获取结束时间
    System.out.println("time cost:" + (endTime - this.startTime) + "ms"); // 输出程序运行时间
    long currentMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long usedMemory = currentMemory - this.startMemory;
    System.out.println("memory cost:" + (usedMemory / 1000) + "k");
  }
}
