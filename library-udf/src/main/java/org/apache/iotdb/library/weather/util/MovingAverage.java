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
package org.apache.iotdb.library.weather.util;

import org.apache.iotdb.library.util.DoubleCircularQueue;

import java.util.ArrayList;

/**
 * This function gets an Arraylist as input and computes moving average. There are blanks at the
 * beginning of the output ArrayList.
 */
public final class MovingAverage {
  public static ArrayList<Double> MvAvg(ArrayList<Double> input, int period) {
    ArrayList<Double> output = new ArrayList<>();
    DoubleCircularQueue q = new DoubleCircularQueue(period);
    Double sum = 0d;
    for (Double aDouble : input) {
      if (q.isFull()) {
        sum -= q.pop();
      }
      sum += aDouble;
      q.push(aDouble);
      if (q.isFull()) {
        output.add(sum / (double) period);
      } else {
        output.add(Double.NaN);
      }
    }
    return output;
  }
}
