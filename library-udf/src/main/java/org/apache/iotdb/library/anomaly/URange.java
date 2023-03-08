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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/** This function is used to detect range anomaly of time series. */
public class URange {
  private double upperBound;
  private double lowerBound;

  public ArrayList<Pair<Long, Double>> getRange(SessionDataSet sds, double ub, double lb)
      throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();
    upperBound = ub;
    lowerBound = lb;
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

    double doubleValue;
    long timestamp;
    timestamp = row.getTimestamp();

    doubleValue = row.getFields().get(0).getDoubleV();
    if (doubleValue > upperBound || doubleValue < lowerBound) {
      res.add(Pair.of(timestamp, doubleValue));
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate() {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
