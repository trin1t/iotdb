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
import org.apache.iotdb.library.anomaly.util.StreamMissDetector;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/** This function is used to detect missing anomalies. */
public class UMissDetect {

  public ArrayList<Pair<Long, Boolean>> getMissDetect(SessionDataSet sds) throws Exception {
    ArrayList<Pair<Long, Boolean>> res = new ArrayList<>();
    beforeStart();

    while (sds.hasNext()) {
      RowRecord row = sds.next();
      res.addAll(transform(row));
    }

    res.addAll(terminate());

    return res;
  }

  private StreamMissDetector detector = new StreamMissDetector(10);

  public void beforeStart() {}

  public ArrayList<Pair<Long, Boolean>> transform(RowRecord row) throws Exception {
    ArrayList<Pair<Long, Boolean>> res = new ArrayList<>();

    detector.insert(row.getTimestamp(), row.getFields().get(0).getDoubleV());
    while (detector.hasNext()) {
      res.add(Pair.of(detector.getOutTime(), detector.getOutValue()));
      detector.next();
    }
    return res;
  }

  public ArrayList<Pair<Long, Boolean>> terminate() throws Exception {
    ArrayList<Pair<Long, Boolean>> res = new ArrayList<>();

    detector.flush();
    while (detector.hasNext()) {
      res.add(Pair.of(detector.getOutTime(), detector.getOutValue()));
      detector.next();
    }
    return res;
  }
}
