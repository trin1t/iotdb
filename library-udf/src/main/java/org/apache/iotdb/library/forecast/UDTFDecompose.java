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
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/** Traditional time series decomposition */
public class UDTFDecompose implements UDTF {
  private ArrayList<Long> timestamp = new ArrayList<>();
  private ArrayList<Double> value = new ArrayList<>();
  private int period;
  private String method;
  private ArrayList<Double> trend = new ArrayList<>();
  private ArrayList<Double> detrended = new ArrayList<>();
  private ArrayList<Double> seasonal = new ArrayList<>();
  private ArrayList<Double> residual = new ArrayList<>();
  private DoubleCircularQueue trendWindow;
  private double windowSum = 0;
  private String output;

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
        .validate(
            method ->
                ((String) method).equalsIgnoreCase("additive")
                    || ((String) method).equalsIgnoreCase("multiplicative"),
            "\"method\" should be \"additive\" or \"multiplicative\".",
            validator.getParameters().getStringOrDefault("method", "additive"))
        .validate(
            output ->
                ((String) output).equalsIgnoreCase("trend")
                    || ((String) output).equalsIgnoreCase("seasoanl")
                    || ((String) output).equalsIgnoreCase("residual"),
            "\"output\" should be \"trend\", \"seasonal\", or \"residual\"",
            validator.getParameters().getStringOrDefault("output", "trend"))
        .validate(
            forecast -> (int) forecast >= 0,
            "\"forecast\" should be a non-negative integer",
            validator.getParameters().getIntOrDefault("forecast", 0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    timestamp.clear();
    value.clear();
    trend.clear();
    detrended.clear();
    residual.clear();
    period = parameters.getInt("period");
    trendWindow =
        period % 2 == 1 ? new DoubleCircularQueue(period) : new DoubleCircularQueue(period + 1);
    windowSum = 0;
    method = parameters.getStringOrDefault("method", "additive");
    output = parameters.getStringOrDefault("output", "trend");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    double v = Util.getValueAsDouble(row);
    if (!Double.isNaN(v)) {
      timestamp.add(row.getTime());
      value.add(v);
      if (trendWindow.isFull()) {
        windowSum -= trendWindow.getHead();
        trendWindow.pop();
      }
      trendWindow.push(v);
      windowSum += v;
      if (trendWindow.isFull()) {
        if (period % 2 == 1) {
          double t = windowSum / (double) period;
          trend.add(t);
          if (method.equalsIgnoreCase("additive")) {
            detrended.add(trendWindow.get((period - 1) / 2) - t);
          } else if (method.equalsIgnoreCase("multiplicative")) {
            detrended.add(trendWindow.get((period - 1) / 2) / t);
          }
        } else {
          double t =
              windowSum / (double) period - (v + trendWindow.getHead()) / (double) period / 2.0d;
          trend.add(t);
          if (method.equalsIgnoreCase("additive")) {
            detrended.add(trendWindow.get(period / 2) - t);
          } else if (method.equalsIgnoreCase("multiplicative")) {
            detrended.add(trendWindow.get(period / 2) / t);
          }
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (value.size() <= period) {
      return;
    }
    Double[] seasonalAverage = new Double[period];
    Integer[] seasons = new Integer[period];
    for (int i = 0; i < period; i++) {
      seasonalAverage[i] = 0D;
      seasons[i] = 0;
    }
    int seasonIndex = period % 2 == 1 ? (period - 1) / 2 : period / 2;
    for (Double aDouble : detrended) {
      seasonalAverage[seasonIndex] += aDouble;
      seasons[seasonIndex]++;
      seasonIndex = (seasonIndex + 1) % period;
    }
    for (int i = 0; i < period; i++) {
      seasonalAverage[i] = seasonalAverage[i] / seasons[i];
    }
    seasonIndex = period % 2 == 1 ? (period - 1) / 2 : period / 2;
    if (method.equalsIgnoreCase("additive")) {
      for (Double aDouble : detrended) {
        residual.add(aDouble - seasonalAverage[seasonIndex]);
        seasonIndex = (seasonIndex + 1) % period;
      }
    } else if (method.equalsIgnoreCase("multiplicative")) {
      for (Double aDouble : detrended) {
        residual.add(aDouble / seasonalAverage[seasonIndex]);
        seasonIndex = (seasonIndex + 1) % period;
      }
    }
    seasonIndex = period % 2 == 1 ? (period - 1) / 2 : period / 2;
    if (output.equalsIgnoreCase("trend")) {
      for (int i = 0; i < trend.size(); i++) {
        collector.putDouble(timestamp.get(i + seasonIndex), trend.get(i));
      }
    } else if (output.equalsIgnoreCase("seasonal")) {
      for (int i = 0; i < trend.size(); i++) {
        collector.putDouble(
            timestamp.get(i + seasonIndex), seasonalAverage[(seasonIndex + i) % period]);
      }
    } else if (output.equalsIgnoreCase("residual")) {
      for (int i = 0; i < trend.size(); i++) {
        collector.putDouble(timestamp.get(i + seasonIndex), residual.get(i));
      }
    }
  }
}
