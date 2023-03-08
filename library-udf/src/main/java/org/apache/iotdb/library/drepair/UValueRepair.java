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

package org.apache.iotdb.library.drepair;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.library.drepair.util.LsGreedy;
import org.apache.iotdb.library.drepair.util.Screen;
import org.apache.iotdb.library.drepair.util.ValueRepair;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/** This function is used to repair the value of the time series. */
public class UValueRepair {
  String method;
  double minSpeed;
  double maxSpeed;
  double center;
  double sigma;
  int windowSize = 10;

  public ArrayList<Pair<Long, Double>> getValueRepair(SessionDataSet sds) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    ArrayList<RowRecord> rows = new ArrayList<>();

    while (sds.hasNext()) {
      RowRecord row = sds.next();
      rows.add(row);
      if (rows.size() == windowSize) {
        res.addAll(transform(rows));
        rows.clear();
      }
    }
    if (rows.size() > 0) {
      res.addAll(transform(rows));
      rows.clear();
    }

    res.addAll(terminate());

    return res;
  }

  public void beforeStart() {}

  public ArrayList<Pair<Long, Double>> transform(ArrayList<RowRecord> rows) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    ValueRepair vr;
    if ("screen".equalsIgnoreCase(method)) {
      Screen screen = new Screen(rows);
      if (!Double.isNaN(minSpeed)) {
        screen.setSmin(minSpeed);
      }
      if (!Double.isNaN(maxSpeed)) {
        screen.setSmax(maxSpeed);
      }
      vr = screen;
    } else if ("lsgreedy".equalsIgnoreCase(method)) {
      LsGreedy lsGreedy = new LsGreedy(rows);
      if (!Double.isNaN(sigma)) {
        lsGreedy.setSigma(sigma);
      }
      lsGreedy.setCenter(center);
      vr = lsGreedy;
    } else {
      throw new Exception("Illegal method.");
    }
    vr.repair();
    double[] repaired = vr.getRepaired();
    long[] time = vr.getTime();
    for (int i = 0; i < time.length; i++) {
      res.add(Pair.of(time[i], repaired[i]));
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate() {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
