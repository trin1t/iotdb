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
import org.apache.iotdb.library.forecast.util.HoltWintersRMSE;
import org.apache.iotdb.library.forecast.util.HoltWintersUtil;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;

import java.util.ArrayList;

/** Describe class here. */
public class UDTFHoltWinters implements UDTF {
  private ArrayList<Long> timestamp = new ArrayList<>();
  private ArrayList<Double> value = new ArrayList<>();
  private int period;
  private String method;
  private String output;
  private boolean auto;
  private int maxEval;
  private double alpha;
  private double beta;
  private double gamma;
  private int forecastNumber;

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
                    || ((String) method).equalsIgnoreCase("multiplicative")
                    || ((String) method).equalsIgnoreCase("linear"),
            "\"method\" should be \"additive\", \"multiplicative\" or \"linear\".",
            validator.getParameters().getStringOrDefault("method", "additive"))
        .validate(
            alpha -> (double) alpha >= 0 && (double) alpha <= 1,
            "\"alpha\" should be within 0 and 1.",
            validator.getParameters().getDoubleOrDefault("alpha", 0.5))
        .validate(
            beta -> (double) beta >= 0 && (double) beta <= 1,
            "\"beta\" should be within 0 and 1.",
            validator.getParameters().getDoubleOrDefault("beta", 0.5))
        .validate(
            gamma -> (double) gamma >= 0 && (double) gamma <= 1,
            "\"gamma\" should be within 0 and 1.",
            validator.getParameters().getDoubleOrDefault("gamma", 0.5))
        .validate(
            maxeval -> (int) maxeval >= 1,
            "\"maxEval\" should be a positive integer.",
            validator.getParameters().getIntOrDefault("maxeval", 250))
        .validate(
            forecastNumber -> (int) forecastNumber >= 0,
            "\"forecastNumber\" should be non negative.",
            validator.getParameters().getIntOrDefault("forecastNumber", 0))
        .validate(
            output ->
                ((String) output).equalsIgnoreCase("coefficients")
                    || ((String) output).equalsIgnoreCase("fitted")
                    || ((String) output).equalsIgnoreCase("trend")
                    || ((String) output).equalsIgnoreCase("seasonal")
                    || ((String) output).equalsIgnoreCase("residual"),
            "\"output\" should be \"coefficients\", \"fitted\", \"trend\",\"seasonal\" or \"residual\"",
            validator.getParameters().getStringOrDefault("output", "fitted"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    timestamp.clear();
    value.clear();
    period = parameters.getInt("period");
    method = parameters.getStringOrDefault("method", "additive");
    auto = parameters.getBooleanOrDefault("auto", true);
    alpha = parameters.getDoubleOrDefault("alpha", 0.5);
    beta = parameters.getDoubleOrDefault("beta", 0.5);
    gamma = parameters.getDoubleOrDefault("gamma", 0.5);
    maxEval = parameters.getIntOrDefault("maxeval", 250);
    forecastNumber = parameters.getIntOrDefault("forecastNumber", 0);
    output = parameters.getStringOrDefault("output", "fitted");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Double v = Util.getValueAsDouble(row);
    if (!v.isNaN()) {
      timestamp.add(row.getTime());
      value.add(v);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (value.size() <= period) {
      return;
    }
    if (auto) {
      if (!method.equalsIgnoreCase("linear")) {
        BOBYQAOptimizer optimizer = new BOBYQAOptimizer(6);
        SimpleBounds bounds = new SimpleBounds(new double[] {0, 0, 0}, new double[] {1, 1, 1});
        InitialGuess guess = new InitialGuess(new double[] {alpha, beta, gamma});
        PointValuePair optimum =
            optimizer.optimize(
                new ObjectiveFunction(
                    new HoltWintersRMSE().setPeriod(period).setType(method).setY(value)),
                GoalType.MINIMIZE,
                bounds,
                guess,
                new MaxEval(maxEval));
        alpha = optimum.getPoint()[0];
        beta = optimum.getPoint()[1];
        gamma = optimum.getPoint()[2];
      } else if (method.equalsIgnoreCase("linear")) {
        BOBYQAOptimizer optimizer = new BOBYQAOptimizer(3);
        SimpleBounds bounds = new SimpleBounds(new double[] {0, 0}, new double[] {1, 1});
        InitialGuess guess = new InitialGuess(new double[] {alpha, beta});
        PointValuePair optimum =
            optimizer.optimize(
                new ObjectiveFunction(
                    new HoltWintersRMSE().setPeriod(period).setType(method).setY(value)),
                GoalType.MINIMIZE,
                bounds,
                guess,
                new MaxEval(maxEval));
        alpha = optimum.getPoint()[0];
        beta = optimum.getPoint()[1];
      }
    }
    HoltWintersUtil model =
        new HoltWintersUtil(
            value, period, new double[] {alpha, beta, gamma}, method, forecastNumber);
    model.fit();
    long interval =
        (timestamp.get(timestamp.size() - 1) - timestamp.get(0)) / (timestamp.size() - 1);
    if (output.equalsIgnoreCase("trend")) {
      ArrayList<Double> trend = model.getTrend();
      for (int i = 0; i < value.size(); i++) {
        collector.putDouble(timestamp.get(i), trend.get(i));
      }
      for (int i = value.size(); i < value.size() + forecastNumber; i++) {
        collector.putDouble(
            (i - value.size() + 1) * interval + timestamp.get(timestamp.size() - 1), trend.get(i));
      }
    } else if (output.equalsIgnoreCase("seasonal")) {
      ArrayList<Double> seasonal = model.getSeasonal();
      for (int i = 0; i < value.size(); i++) {
        collector.putDouble(timestamp.get(i), seasonal.get(i));
      }
      for (int i = value.size(); i < value.size() + forecastNumber; i++) {
        collector.putDouble(
            (i - value.size() + 1) * interval + timestamp.get(timestamp.size() - 1),
            seasonal.get(i));
      }
    } else if (output.equalsIgnoreCase("fitted")) {
      ArrayList<Double> fitted = model.getFitted();
      for (int i = 0; i < value.size(); i++) {
        collector.putDouble(timestamp.get(i), fitted.get(i));
      }
      for (int i = value.size(); i < value.size() + forecastNumber; i++) {
        collector.putDouble(
            (i - value.size() + 1) * interval + timestamp.get(timestamp.size() - 1), fitted.get(i));
      }
    } else if (output.equalsIgnoreCase("residual")) {
      ArrayList<Double> residual = model.getResidual();
      for (int i = 0; i < value.size(); i++) {
        collector.putDouble(timestamp.get(i), residual.get(i));
      }
    } else if (output.equalsIgnoreCase("coefficients")) {
      collector.putDouble(1, alpha);
      collector.putDouble(2, beta);
      if (!method.equalsIgnoreCase("linear")) {
        collector.putDouble(3, gamma);
      }
    }
  }
}
