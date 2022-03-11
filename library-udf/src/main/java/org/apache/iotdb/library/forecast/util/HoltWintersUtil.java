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
package org.apache.iotdb.library.forecast.util;

import java.util.ArrayList;

/** Fit Holt-Winters model. */
public class HoltWintersUtil {
  ArrayList<Double> a = new ArrayList<>();
  ArrayList<Double> b = new ArrayList<>();
  ArrayList<Double> y = new ArrayList<>();
  ArrayList<Double> s = new ArrayList<>();
  String method;
  int period;
  double alpha, beta, gamma;
  double[] Y;
  int forecast;

  public HoltWintersUtil(
      double[] data, int period, double[] params, String method, int forecastNumber) {
    a.clear();
    b.clear();
    y.clear();
    s.clear();
    this.method = method;
    alpha = params[0];
    beta = params[1];
    if (!method.equalsIgnoreCase("linear")) {
      gamma = params[2];
    }
    this.method = method;
    Y = new double[data.length];
    System.arraycopy(data, 0, Y, 0, data.length);
    this.period = period;
    forecast = forecastNumber;
  }

  public HoltWintersUtil(
      ArrayList<Double> data, int period, double[] params, String method, int forecastNumber) {
    a.clear();
    b.clear();
    y.clear();
    s.clear();
    this.method = method;
    alpha = params[0];
    beta = params[1];
    if (!method.equalsIgnoreCase("linear")) {
      gamma = params[2];
    }
    Y = data.stream().mapToDouble(Double::valueOf).toArray();
    this.period = period;
    forecast = forecastNumber;
  }

  public void fit() {
    if (method.equalsIgnoreCase("linear")) {
      a.add(Y[0]);
      b.add(Y[1] - Y[0]);
      y.add(a.get(0) + b.get(0));
      for (int i = 0; i < Y.length; i++) {
        a.add(alpha * Y[i] + (1 - alpha) * (a.get(i) + b.get(i)));
        b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
        y.add(a.get(i + 1) + b.get(i + 1));
      }
      for (int i = Y.length; i < Y.length + forecast; i++) {
        a.add(alpha * y.get(i) + (1 - alpha) * (a.get(i) + b.get(i)));
        b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
        y.add(a.get(i + 1) + b.get(i + 1));
      }
    } else {
      double YSum1 = 0;
      for (int i = 0; i < period; i++) {
        YSum1 += Y[i];
      }
      a.add(YSum1 / (double) period);
      double YSum2 = 0;
      for (int i = period; i < 2 * period; i++) {
        YSum2 += Y[i];
      }
      b.add((YSum2 - YSum1) / (double) period / (double) period);
      if (method.equalsIgnoreCase("additive")) {
        for (int i = 0; i < period; i++) {
          s.add(Y[i] - a.get(0));
        }
        y.add(a.get(0) + b.get(0) + s.get(0));
        for (int i = 0; i < Y.length; i++) {
          a.add(alpha * (Y[i] - s.get(i)) + (1 - alpha) * (a.get(i) + b.get(i)));
          b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
          s.add(gamma * (Y[i] - a.get(i) - b.get(i)) + (1 - gamma) * s.get(i));
          y.add(a.get(i + 1) + b.get(i + 1) + s.get(i + 1));
        }
        for (int i = Y.length; i < Y.length + forecast; i++) {
          a.add(alpha * (y.get(i) - s.get(i)) + (1 - alpha) * (a.get(i) + b.get(i)));
          b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
          s.add(gamma * (y.get(i) - a.get(i) - b.get(i)) + (1 - gamma) * s.get(i));
          y.add(a.get(i + 1) + b.get(i + 1) + s.get(i + 1));
        }
      } else if (method.equalsIgnoreCase("multiplicative")) {
        for (int i = 0; i < period; i++) {
          s.add(Y[i] / a.get(0));
        }
        y.add((a.get(0) + b.get(0)) * s.get(0));
        for (int i = 0; i < Y.length; i++) {
          a.add(alpha * (Y[i] / s.get(i)) + (1 - alpha) * (a.get(i) + b.get(i)));
          b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
          s.add(gamma * (Y[i] / (a.get(i) - b.get(i))) + (1 - gamma) * s.get(i));
          y.add((a.get(i + 1) + b.get(i + 1)) * s.get(i + 1));
        }
        for (int i = Y.length; i < Y.length + forecast; i++) {
          a.add(alpha * (y.get(i) / s.get(i)) + (1 - alpha) * (a.get(i) + b.get(i)));
          b.add(beta * (a.get(i + 1) - a.get(i)) + (1 - beta) * b.get(i));
          s.add(gamma * (y.get(i) / (a.get(i) - b.get(i))) + (1 - gamma) * s.get(i));
          y.add((a.get(i + 1) + b.get(i + 1)) * s.get(i + 1));
        }
      } else {
        System.out.println("Wrong method type. Should be additive, multiplicative or linear.");
      }
    }
  }

  public ArrayList<Double> getFitted() {
    return y;
  }

  public ArrayList<Double> getSeasonal() {
    return s;
  }

  public ArrayList<Double> getTrend() {
    ArrayList<Double> trend = new ArrayList<>();
    for (int i = 0; i < a.size(); i++) {
      trend.add(a.get(i) + b.get(i));
    }
    return trend;
  }

  public ArrayList<Double> getResidual() {
    ArrayList<Double> residual = new ArrayList<>();
    for (int i = 0; i < Y.length; i++) {
      residual.add(Y[i] - y.get(i));
    }
    return residual;
  }
}
