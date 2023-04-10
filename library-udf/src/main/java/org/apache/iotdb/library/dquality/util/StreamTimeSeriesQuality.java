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

package org.apache.iotdb.library.dquality.util;

import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;

import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.util.ArrayList;
import java.util.Arrays;

/** Class for computing data quality index. */
public class StreamTimeSeriesQuality {
  private int windowSize = 100;
  private int slidingStep = 10;
  private int slidingPoints = 0;
  private int cnt = 0; // total number of sliding windows
  private String currentFunction;
  // timestamp error variable
  private long standardInterval = -1;
  private int missingCnt = 0; // number of missing points
  private int specialCnt = 0; // number of special values
  private int latencyCnt = 0; // number of latency points
  private int redundancyCnt = 0; // number of redundancy points
  private LongCircularQueue timeErrorType; // 0 = no error, 1 = latency, 2 = redundancy
  private int latencyNum = 0;
  private int missingNum = 0;
  private LongCircularQueue missings;
  private double shutdownFactor =
      20; // if time interval is larger than this times standard interval, it is considered shutdown
  // validity error variable
  private int valueCnt = 0; // number of out of range points in a window
  private int variationCnt = 0; // number of variation out of range points in a window
  private int speedCnt = 0; // number of speed out of range points in a window
  private int speedchangeCnt = 0; // number of acceleration out of range points in a window
  // timestamp records
  private LongCircularQueue timeList; // original timestamps
  private long expectedFirstTime; // theoretical timestamps
  private LongCircularQueue intervalList; // original 1-order timestamp differences
  private DoubleCircularQueue originList; // original values
  private long base = Long.MAX_VALUE / windowSize; // standard timestamp interval in this window

  public StreamTimeSeriesQuality(String fun, int w, int s, long stdintv) throws Exception {
    currentFunction = fun;
    windowSize = w;
    slidingStep = s;
    standardInterval = stdintv;
    timeList = new LongCircularQueue(windowSize);
    timeErrorType = new LongCircularQueue(windowSize);
    intervalList = new LongCircularQueue(windowSize - 1);
    originList = new DoubleCircularQueue(windowSize);
    missings = new LongCircularQueue(windowSize / slidingStep);
  }

  public void updateStreamTimeSeriesQuality(Row row) throws Exception {
    double v = Util.getValueAsDouble(row);
    long t = row.getTime();
    boolean specialValue = false;
    if (Double.isFinite(v)) {
      originList.push(v);
    } else { // processing NAN，INF
      specialCnt++;
      specialValue = true;
    }
    if (originList.isFull()) {
      timeList.pop();
      intervalList.pop();
      originList.pop();
    }
    if (specialValue) {
      originList.push(Double.NaN);
    } else {
      originList.push(v);
    }
    intervalList.push(t - timeList.getTail());
    timeList.push(t);
    // clear data before shutdown period
    // if shutdown occurs in the first window, please change window size
    if (intervalList.getTail() > base * shutdownFactor
        && !currentFunction.equalsIgnoreCase("validity")) {
      while (!intervalList.isEmpty()) {
        originList.pop();
        timeList.pop();
        intervalList.pop();
        if (timeErrorType.pop() == 1) {
          latencyNum--;
        }
      }
      slidingPoints = 0;
    }
    if (originList.isFull()) {
      if (slidingPoints == slidingStep - 1 || cnt == 0) {
        cnt++;
        slidingPoints++;
        missingNum -= missings.pop();
        if (!currentFunction.equalsIgnoreCase("validity")) {
          updateTimestampQuality();
        } else {
          updateValidityQuality();
        }
        slidingPoints = 0;
      } else {
        slidingPoints++;
        if (!currentFunction.equalsIgnoreCase("validity") && timeErrorType.pop() == 1) {
          latencyNum--;
        }
      }
    }
  }

  private void updateTimestampQuality() throws Exception {
    if (cnt == 1) { // step to initialize
      if (standardInterval > 0) {
        base = standardInterval;
      } else {
        base =
            Math.round(
                new Median()
                    .evaluate(
                        Arrays.stream(intervalList.getData())
                            .mapToDouble(Double::valueOf)
                            .toArray()));
      }
      findExpectedTime();
      matchTimestamp();
    } else {
      matchNewTimestamp();
    }
    if (latencyNum >= 0.4 * windowSize || missingNum >= 0.4 * windowSize) {
      base =
          Math.round(
              new Median()
                  .evaluate(
                      Arrays.stream(intervalList.getData())
                          .mapToDouble(Double::valueOf)
                          .toArray()));
      findExpectedTime();
      missingNum = 0;
      latencyNum = 0;
      matchTimestamp();
    }
  }

  private void findExpectedTime() {
    ArrayList<Long> timestampError = new ArrayList<>();
    long initTime = timeList.getHead();
    for (int i = 0; i < timeList.getSize(); i++) {
      long delta = (timeList.get(i) - initTime) % base;
      if (delta > base / 2.0) {
        delta -= base;
      }
      timestampError.add(delta);
    }
    double med =
        new Median()
            .evaluate(timestampError.stream().mapToDouble(Double::valueOf).toArray()); // todo
    expectedFirstTime = initTime - Math.round(med);
  }

  private void matchTimestamp() { // conducts on a complete window
    if (missings.isEmpty()) {
      for (int i = 0; i < windowSize / slidingStep; i++) {
        missings.push(0);
      }
    } else {
      for (int i = 0; i < windowSize / slidingStep; i++) {
        missingCnt -= missings.get(i);
        missings.set(i, 0);
      }
    }
    while (!timeErrorType.isEmpty()) {
      long type = timeErrorType.pop();
      switch ((int) type) {
        case 1:
          latencyCnt--;
          break;
        case 2:
          redundancyCnt--;
          break;
        default:
          break;
      }
    }
    int expectedPointNumber =
        Math.round((timeList.getTail() - timeList.getHead()) / (float) base) + 1;
    int originPointIndex = 0;
    long t = timeList.getHead();
    long expectedT = ((t - expectedFirstTime) / base) * base + expectedFirstTime;
    for (int i = 0; i < expectedPointNumber; i++) {
      if (t - expectedT > base) {
        missingCnt++;
        missingNum++;
        missings.set(
            originPointIndex / slidingStep, missings.get(originPointIndex / slidingStep) + 1);
        expectedT += base;
      } else if (t > expectedT) {
        latencyCnt++;
        latencyNum++;
        timeErrorType.push(1);
        originPointIndex++;
        if (originPointIndex >= timeList.getSize()) {
          expectedFirstTime = expectedT;
          return;
        }
        t = timeList.get(originPointIndex);
        while (t < expectedT + base) {
          redundancyCnt++;
          timeErrorType.push(2);
          originPointIndex++;
          if (originPointIndex >= timeList.getSize()) {
            expectedFirstTime = expectedT;
            return;
          }
          t = timeList.get(originPointIndex);
        }
      } else {
        timeErrorType.push(0);
        originPointIndex++;
        if (originPointIndex >= timeList.getSize()) {
          expectedFirstTime = expectedT;
          return;
        }
        t = timeList.get(originPointIndex);
      }
      expectedT += base;
    }
    expectedFirstTime = expectedT;
  }

  private void matchNewTimestamp() {
    missings.push(0);
    int expectedPointNumber =
        Math.round((timeList.getTail() - timeList.get(windowSize - slidingStep + 1)) / (float) base)
            + 1;
    int originPointIndex = 0;
    long t = timeList.get(windowSize - slidingStep + 1);
    long expectedT = expectedFirstTime;
    for (int i = 0; i < expectedPointNumber; i++) {
      if (t - expectedT > base) {
        missingCnt++;
        missingNum++;
        missings.set(missings.getSize() - 1, missings.getTail() + 1);
        expectedT += base;
      } else if (t > expectedT) {
        latencyCnt++;
        latencyNum++;
        timeErrorType.push(1);
        originPointIndex++;
        if (originPointIndex >= timeList.getSize()) {
          return;
        }
        t = timeList.get(originPointIndex);
        while (t < expectedT + base) {
          redundancyCnt++;
          timeErrorType.push(2);
          originPointIndex++;
          if (originPointIndex >= timeList.getSize()) {
            return;
          }
          t = timeList.get(originPointIndex);
        }
      } else {
        timeErrorType.push(0);
        originPointIndex++;
        if (originPointIndex >= timeList.getSize()) {
          return;
        }
        t = timeList.get(originPointIndex);
      }
      expectedT += base;
    }
  }

  private void updateValidityQuality() throws Exception {
    processNaN();
    valueDetect();
  }

  /** linear interpolation of NaN */
  private void processNaN() throws Exception {
    int n = windowSize;
    int index1 = slidingPoints == slidingStep ? windowSize - slidingStep : 0;
    int index2;
    while (index1 < n && Double.isNaN(originList.get(index1))) {
      index1++;
    }
    index2 = index1 + 1;
    while (index2 < n && Double.isNaN(originList.get(index2))) {
      index2++;
    }
    if (index2 >= n) {
      throw new Exception("At least two non-NaN values are needed");
    }
    if (n == windowSize) {
      // interpolation at the beginning of the series
      for (int i = 0; i < index2; i++) {
        originList.set(
            i,
            originList.get(index1)
                + (originList.get(index2) - originList.get(index1))
                    * (timeList.get(i) - timeList.get(index1))
                    / (timeList.get(index2) - timeList.get(index1)));
      }
    }
    // interpolation at the middle of the series
    for (int i = index2 + 1; i < n; i++) {
      if (!Double.isNaN(originList.get(i))) {
        index1 = index2;
        index2 = i;
        for (int j = index1 + 1; j < index2; j++) {
          originList.set(
              j,
              originList.get(index1)
                  + (originList.get(index2) - originList.get(index1))
                      * (timeList.get(j) - timeList.get(index1))
                      / (timeList.get(index2) - timeList.get(index1)));
        }
      }
    }
    // interpolation at the end of the series
    for (int i = index2 + 1; i < n; i++) {
      originList.set(
          i,
          originList.get(index1)
              + (originList.get(index2)
                  - originList.get(index1)
                      * (timeList.get(i) - timeList.get(index1))
                      / (timeList.get(index2) - timeList.get(index1))));
    }
  }

  /** preparation for validity */
  public void valueDetect() {
    int k = 3;
    if (slidingPoints == slidingStep) {
      double[] valueList = new double[slidingStep + 1];
      for (int i = windowSize - slidingStep - 1; i < windowSize; i++) {
        valueList[i - windowSize + slidingStep + 1] = originList.get(i);
      }
      valueCnt += findOutliers(valueList, k);
      // range anomaly
      double[] variation = Util.variation(valueList);
      variationCnt += findOutliers(variation, k);
      // speed anomaly
      double[] time = new double[slidingStep + 1];
      for (int i = windowSize - slidingStep - 1; i < windowSize; i++) {
        time[i - windowSize + slidingStep + 1] =
            timeList.get(i) - timeList.get(windowSize - slidingStep - 1);
      }
      double[] speed = Util.speed(valueList, time);
      speedCnt += findOutliers(speed, k);
      // acceleration anomaly
      double[] speedchange = Util.variation(speed);
      speedchangeCnt += findOutliers(speedchange, k);
    } else {

    }
  }

  /** return number of points lie out of median +- k * MAD */
  private int findOutliers(double[] value, double k) {
    Median median = new Median();
    double mid = median.evaluate(value);
    double sigma = Util.mad(value);
    int num = 0;
    for (double v : value) {
      if (Math.abs(v - mid) > k * sigma) {
        num++;
      }
    }
    return num;
  }

  public double getCompleteness() {
    return 1 - (missingCnt + specialCnt) * 1.0 / (cnt + missingCnt);
  }

  public double getConsistency() {
    return 1 - redundancyCnt * 1.0 / cnt;
  }

  public double getTimeliness() {
    return 1 - latencyCnt * 1.0 / cnt;
  }

  public double getValidity() {
    return 1 - (valueCnt + variationCnt + speedCnt + speedchangeCnt) * 0.25 / cnt;
  }

  public double getDataQuality() {
    if (currentFunction.equalsIgnoreCase("completeness")) {
      return getCompleteness();
    }
    if (currentFunction.equalsIgnoreCase("consistency")) {
      return getConsistency();
    }
    if (currentFunction.equalsIgnoreCase("timeliness")) {
      return getTimeliness();
    }
    if (currentFunction.equalsIgnoreCase("validity")) {
      return getValidity();
    } else {
      return Double.NaN;
    }
  }

  public int getSlidingPoints() {
    return slidingPoints;
  }
}
