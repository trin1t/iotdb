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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/*
This function is used to detect distance-based anomalies.
*/
public class UOutlier {
  private int k;
  private double r;
  private int w;
  private int s;
  private int i;
  private ArrayList<Long> currentTimeWindow = new ArrayList<>();
  private ArrayList<Double> currentValueWindow = new ArrayList<>();
  private Map<Long, Double> outliers = new HashMap<>();

  public ArrayList<Pair<Long, Double>> getOutlier(SessionDataSet sds) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    while (sds.hasNext()) {
      RowRecord row = sds.next();
      res.addAll(transform(row));
    }

    res.addAll(terminate());

    return res;
  }

  public void beforeStart() {};

  public ArrayList<Pair<Long, Double>> transform(RowRecord row) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    // if (!row.isNull(0)) {
    if (!row.getFields().isEmpty()) {
      if (i >= w && (i - w) % s == 0) detect();

      if (i >= w) {
        currentValueWindow.remove(0);
        currentTimeWindow.remove(0);
      }
      currentTimeWindow.add(row.getTimestamp());
      currentValueWindow.add(row.getFields().get(0).getDoubleV());
      i += 1;
    }
    return res;
  }

  private void detect() {
    for (int j = 0; j < w; j++) {
      int cnt = 0;
      for (int l = 0; l < w; l++)
        if (Math.abs(currentValueWindow.get(j) - currentValueWindow.get(l)) <= this.r) cnt++;
      if (cnt < this.k && !outliers.containsKey(currentTimeWindow.get(j)))
        outliers.put(currentTimeWindow.get(j), currentValueWindow.get(j));
    }
  }

  public ArrayList<Pair<Long, Double>> terminate() {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    for (Long time :
        outliers.keySet().stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList())) {
      res.add(Pair.of(time, outliers.get(time)));
    }
    return res;
  }
}
