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

package org.apache.iotdb.library.anomaly;

import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.library.util.CircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/** This function detects outliers which lies over average +/- k * sigma. */
public class UKSigma {
  private double mean = 0.0;
  private double var = 0.0;
  private double sumX2 = 0.0;
  private double sumX1 = 0.0;
  private double multipleK = 3;
  private int windowSize = 10000;
  private CircularQueue<Object> v = new CircularQueue<>(windowSize);
  private LongCircularQueue t = new LongCircularQueue(windowSize);

  public ArrayList<Pair<Long, Double>> getKSigma(SessionDataSet sds) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    while (sds.hasNext()) {
      RowRecord row = sds.next();
      res.addAll(transform(row));
    }

    res.addAll(terminate());

    return res;
  }

  public void beforeStart() {}

  public ArrayList<Pair<Long, Double>> transform(RowRecord row) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    double value = row.getFields().get(0).getDoubleV();
    long timestamp = row.getTimestamp();
    if (Double.isFinite(value) && !Double.isNaN(value)) {
      if (v.isFull()) {
        double frontValue = Double.parseDouble(v.pop().toString());
        // v.push(row.getDouble(0));
        v.push(row.getFields().get(0).getDoubleV());
        t.pop();
        t.push(timestamp);
        this.sumX1 = this.sumX1 - frontValue + value;
        this.sumX2 = this.sumX2 - frontValue * frontValue + value * value;
        this.mean = this.sumX1 / v.getSize();
        this.var = this.sumX2 / v.getSize() - this.mean * this.mean;
        if (Math.abs(value - mean)
            > multipleK * Math.sqrt(this.var * v.getSize() / (v.getSize() - 1))) {
          res.add(Pair.of(timestamp, row.getFields().get(0).getDoubleV()));
        }
      } else {
        // v.push(row.getDouble(0));
        v.push(row.getFields().get(0).getDoubleV());
        t.push(timestamp);
        this.sumX1 = this.sumX1 + value;
        this.sumX2 = this.sumX2 + value * value;
        this.mean = this.sumX1 / v.getSize();
        this.var = this.sumX2 / v.getSize() - this.mean * this.mean;
        if (v.getSize() == this.windowSize) {
          double stddev = Math.sqrt(this.var * v.getSize() / (v.getSize() - 1));
          for (int i = 0; i < v.getSize(); i++) {
            Object v = this.v.get(i);
            timestamp = this.t.get(i);
            if (Math.abs(Double.parseDouble(v.toString()) - mean) > multipleK * stddev) {
              // Util.putValue(collector, dataType, timestamp, v);
              res.add(Pair.of(timestamp, Double.parseDouble(v.toString())));
            }
          }
        }
      }
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate() throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    if (!v.isFull() && v.getSize() > 1) {
      double stddev = Math.sqrt(this.var * v.getSize() / (v.getSize() - 1));
      for (int i = 0; i < v.getSize(); i++) {
        Object v = this.v.get(i);
        long timestamp = this.t.get(i);
        if (Math.abs(Double.parseDouble(v.toString()) - mean) > multipleK * stddev) {
          res.add(Pair.of(timestamp, Double.parseDouble(v.toString())));
        }
      }
    }
    return res;
  }
}
