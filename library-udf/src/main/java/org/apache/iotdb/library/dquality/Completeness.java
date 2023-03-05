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

package org.apache.iotdb.library.dquality;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.dquality.util.TimeSeriesQuality;
import org.apache.iotdb.isession.SessionDataSet;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/** This function calculates completeness of input series. */
public class Completeness{
  // 简化初始化设置，配置默认参数，也没有validate环节
  private boolean downtime = true;
  int windowSize = 10;
  // 主入口
  public ArrayList<Pair<Long, Double>> getCompleteness(SessionDataSet sds)
      throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    ArrayList<RowRecord> rows = new ArrayList<>();
    while(sds.hasNext()){
      RowRecord row = sds.next();
      rows.add(row);
      if(rows.size() == windowSize){
        res.addAll(transform(rows));
        rows.clear();
      }
    }
    if(rows.size() > 0){
      res.addAll(transform(rows));
      rows.clear();
    }

    res.addAll(terminate());

    return res;
  }


  // 为了方便改，这里用了与UDF相同的执行结构
  public void beforeStart(){
    return;
  }

  public ArrayList<Pair<Long, Double>> transform(ArrayList<RowRecord> rows) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    try {
      if (rows.size() > TimeSeriesQuality.windowSize) {
        TimeSeriesQuality tsq = new TimeSeriesQuality(rows);
        tsq.setDowntime(downtime);
        tsq.timeDetect();
        res.add(Pair.of(rows.get(0).getTimestamp(), tsq.getCompleteness()));
      }
    } catch (IOException ex) {
      Logger.getLogger(Completeness.class.getName()).log(Level.SEVERE, null, ex);
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate(){
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
