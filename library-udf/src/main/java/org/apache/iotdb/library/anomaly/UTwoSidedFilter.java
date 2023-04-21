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
import org.apache.iotdb.library.anomaly.util.WindowDetect;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/**
 * The function is used to filter anomalies of a numeric time series based on two-sided window
 * detection.
 */
public class UTwoSidedFilter {
  private double len = 5;
  private double threshold = 0.4;
  int windowSize = 10;

  public ArrayList<Pair<Long, Double>> getTwoSidedFilter(SessionDataSet sds) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    ArrayList<RowRecord> rows = new ArrayList<>();

    while (sds.hasNext()) {
      RowRecord row = sds.next();
      rows.add(row);
      if (rows.size() == windowSize) {
        transform(rows, res);
        rows.clear();
      }
    }
    if (rows.size() > 0) {
      transform(rows,res);
      rows.clear();
    }

    terminate(res);
    return res;
  }

  public void beforeStart() {}

  public void transform(ArrayList<RowRecord> rows, ArrayList<Pair<Long, Double>> res) throws Exception {

    WindowDetect wd = new WindowDetect(rows, len, threshold);
    double[] repaired = wd.getRepaired();
    long[] time = wd.getTime();
    for (int i = 0; i < time.length; i++) {
      res.add(Pair.of(time[i], repaired[i]));
    }
  }

  public void terminate(ArrayList<Pair<Long, Double>> res) {
  }
}
