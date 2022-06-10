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

/** This function collects the historical value. */
public class UDTFHistoricalData implements UDTF {
  private final ArrayList<Double> value = new ArrayList<>();
  private final ArrayList<Long> timestamp = new ArrayList<>();
  private int period;
  private int current_year;
  private long start;
  private long end;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
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
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long t = row.getTime();
    Double v = Util.getValueAsDouble(row);
    Date date = new Date(t);
    int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
    String year_str = Integer.toString(year);
    if (current_year - year <= period) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String date_str = sdf.format(date);
      String newDate_str = date_str.replace(date_str.substring(0, 4), year_str);
      Date newDate = sdf.parse(newDate_str);
      long t2 = newDate.getTime();
      if (t2 > start && t2 < end) {
        value.add(v);
        timestamp.add(t);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for (int i = 0; i < value.size(); i++) {
      collector.putDouble(timestamp.get(i), value.get(i));
    }
  }
}
