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

import org.apache.commons.math3.analysis.MultivariateFunction;

import java.util.ArrayList;

/** Objective function for Holt-Winters. Designed for apache-commons-math3.6.1 */
public class HoltWintersRMSE implements MultivariateFunction {
  double[] Y;
  String type;
  int period;

  public HoltWintersRMSE setY(double[] data) {
    Y = new double[data.length];
    System.arraycopy(data, 0, Y, 0, data.length);
    return this;
  }

  public HoltWintersRMSE setY(ArrayList<Double> data) {
    Y = data.stream().mapToDouble(Double::valueOf).toArray();
    return this;
  }

  public HoltWintersRMSE setType(String method) {
    type = method;
    return this;
  }

  public HoltWintersRMSE setPeriod(int t) {
    period = t;
    return this;
  }

  @Override
  public double value(double[] params) {
    double alpha = params[0];
    double beta = params[1];
    double gamma = 0;
    if (!type.equalsIgnoreCase("linear")) {
      gamma = params[2];
    }
    ArrayList<Double> a = new ArrayList<>();
    ArrayList<Double> b = new ArrayList<>();
    ArrayList<Double> y = new ArrayList<>();
    ArrayList<Double> s = new ArrayList<>();
    if (type.equalsIgnoreCase("linear")) {
      a.add(Y[0]);
      b.add(Y[1] - Y[0]);
      y.add(a.get(0) + b.get(0));
      for (int i = 0; i < Y.length; i++) {
        a.add(alpha * Y[i] + (1 - alpha) * (a.get(i) + b.get(i)));
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

      if (type.equalsIgnoreCase("additive")) {
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
      } else if (type.equalsIgnoreCase("multiplicative")) {
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
      } else {
        System.out.println("Wrong method type. Should be additive, multiplicative or linear.");
        return 0;
      }
    }
    double ss = 0;
    for (int i = 0; i < Y.length; i++) {
      ss += (Y[i] - y.get(i)) * (Y[i] - y.get(i));
    }
    return Math.sqrt(ss / (double) Y.length);
  }
}
