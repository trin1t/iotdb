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
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.text.SimpleDateFormat;
import java.util.*;

/** This function calculates the average of some value in history. */
public class UDTFHistoryRank implements UDTF {
  private ArrayList<Double> value;
  private ArrayList<Double> sort_value;
  private ArrayList<Long> timestamp;
  private HashMap<Integer, Integer> count;
  private HashMap<Double, Integer> sort_order;
  private HashMap<Integer, ArrayList<Double>> sort_value_by_day;
  private HashMap<Integer, HashMap<Double, Integer>> sort_order_by_day;

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
    value = new ArrayList<>();
    timestamp = new ArrayList<>();
    count = new HashMap<>();
    sort_value_by_day = new HashMap<>();
    sort_order_by_day = new HashMap<>();
    for (int d = 1; d <= 366; d++) {
      count.put(d, 0);
      sort_value = new ArrayList<>();
      sort_value_by_day.put(d, sort_value);
      sort_order = new HashMap<>();
      sort_order_by_day.put(d, sort_order);
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Double v = Util.getValueAsDouble(row);
    value.add(v);
    Date date = new Date();
    date.setTime(row.getTime());
    timestamp.add(row.getTime());
    SimpleDateFormat ft = new SimpleDateFormat("MMdd");
    Integer day = Integer.parseInt(ft.format(date));
    count.put(day, count.get(day) + 1);
    sort_value = sort_value_by_day.get(day);
    sort_value.add(v);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    {
      SimpleDateFormat ft = new SimpleDateFormat("MMdd");
      Date date = new Date();
      for (int m = 1; m <= 12; m++) {
        for (int d = 1; d <= 31; d++) {
          if (count.get(m * 100 + d) > 0) {
            sort_value = sort_value_by_day.get(m * 100 + d);
            Collections.sort(sort_value);
            sort_value_by_day.put(m * 100 + d, sort_value);
            sort_order = sort_order_by_day.get(m * 100 + d);
            for (int i = 0; i < sort_value.size(); i++) {
              sort_order.put(sort_value.get(i), i + 1);
            }
            sort_order_by_day.put(m * 100 + d, sort_order);
          }
        }
      }
      for (int i = 0; i < value.size(); i++) {
        date.setTime(timestamp.get(i));
        if (count.get(i) > 0) {
          int day = Integer.parseInt(ft.format(date));
          sort_value = sort_value_by_day.get(day);
          sort_order = sort_order_by_day.get(day);
          collector.putDouble(timestamp.get(i), sort_order.get(value.get(i)));
        }
      }
    }
  }
}
