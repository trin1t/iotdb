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
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.library.weather.util.MovingAverage;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/** This function conducts optimal climate normals (OCN) forecast. */
// TODO: support different criteria
public class UDTFOCN implements UDTF {
  private int period;
  private String criterion;
  private String output;
  private ArrayList<ArrayList<Double>> value;
  private int phase;
  private int lb;
  private int ub;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            period -> (int) period > 0,
            "Parameter \"period\" should be positive integer",
            validator.getParameters().getInt("period"))
        .validate(
            output ->
                ((String) output).equalsIgnoreCase("kk")
                    || ((String) output).equalsIgnoreCase("forecast"),
            "Parameter \"output\" should be \"kk\" or \"forecast\"",
            validator.getParameters().getStringOrDefault("output", "forecast"));
    if (validator.getParameters().hasAttribute("lower_bound")) {
      validator.validate(
          lower_bound -> (int) lower_bound > 0,
          "Parameter \"lower_bound\" should be a positive integer.",
          validator.getParameters().getIntOrDefault("lower_bound", 1));
    }
    if (validator.getParameters().hasAttribute("upper_bound")) {
      validator.validate(
          upper_bound -> (int) upper_bound > 0,
          "Parameter \"upper_bound\" should be a positive integer.",
          validator.getParameters().getIntOrDefault("upper_bound", Integer.MAX_VALUE));
    }
    if (validator.getParameters().hasAttribute("lower_bound")
        && validator.getParameters().hasAttribute("upper_bound")) {
      validator.validate(
          params -> (double) params[0] < (double) params[1],
          "parameter \"lower_bound\" should be smaller than \"upper_bound\".",
          validator.getParameters().getIntOrDefault("lower_bound", 1),
          validator.getParameters().getIntOrDefault("upper_bound", Integer.MAX_VALUE));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    output = parameters.getStringOrDefault("output", "forecast");
    if (output.equalsIgnoreCase("kk")) {
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.INT32);
    } else if (output.equalsIgnoreCase("forecast")) {
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.DOUBLE);
    } else {
      throw new UDFException("Invalid input");
    }
    period = parameters.getInt("period");
    criterion = parameters.getStringOrDefault("criterion", "abs");
    lb = parameters.getIntOrDefault("lower_bound", 0);
    ub = parameters.getIntOrDefault("upper_bound", Integer.MAX_VALUE);
    value = new ArrayList<>();
    for (int i = 0; i < period; i++) {
      value.add(new ArrayList<>());
    }
    phase = 0;
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    value.get(phase).add(Util.getValueAsDouble(row));
    phase = (phase + 1) % period;
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    // compute average of kk periods
    int kk; // number of periods to calculate average
    int bestkk = 0;
    double bestcit = Double.MAX_VALUE;
    ArrayList<Double> forecast = new ArrayList<>();
    ub = Math.min(ub, value.get(period - 1).size());
    for (kk = lb; kk <= ub; kk++) {
      double cit = 0d;
      if (criterion.equalsIgnoreCase("abs")) {
        int count = 0;
        ArrayList<Double> f = new ArrayList<>();
        for (int i = 0; i < period; i++) {
          ArrayList<Double> ma = MovingAverage.MvAvg(value.get(i), period);
          for (int j = kk * period; j < value.get(i).size(); j++) {
            cit += Math.abs(value.get(i).get(j) - ma.get(j - period));
            count++;
          }
          f.add(ma.get(value.get(i).size() - 1));
        }
        cit /= count;
        if (cit < bestcit) {
          bestcit = cit;
          bestkk = kk;
          forecast = new ArrayList<>(f);
        }
      }
    }
    if (output.equalsIgnoreCase("kk")) {
      collector.putInt(0, bestkk);
    } else if (output.equalsIgnoreCase("forecast")) {
      for (int i = 1; i <= period; i++) {
        collector.putDouble(i, forecast.get(i));
      }
    }
  }
}
