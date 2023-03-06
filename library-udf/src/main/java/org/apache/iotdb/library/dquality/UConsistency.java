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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.dquality.util.TimeSeriesQuality;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This function calculates consistency of input series. */
public class UConsistency{
  private boolean downtime = true;
  int windowSize = 10;

  public ArrayList<Pair<Long, Double>> getConsistency(SessionDataset sds) throws Exception{
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    beforeStart();

    ArrayList<RowRecord> rows = new ArrayList<>();

    while(sds.hasNext()){
      RowRecord row = sds.next();
      rows.add(row);
      if(rows.size()==windowSize){
        res.addAll(transform(rows));
        rows.clear();
      }
    }
    if(rows.size()>0){
      res.addAll(transform(rows));
      rows.clear();
    }

    res.addAll(terminate());

    return res;
  }

  public void beforeStart() {
    return;
  }

  public ArrayList<Pair<Long, Double>> transform(ArrayList<RowRecord> rows) throws Exception{
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    try {
      if (rows.size()> TimeSeriesQuality.windowSize){
        TimeSeriesQuality tsq = new TimeSeriesQuality(rows);
        tsq.setDowntime(downtime);
        tsq.timeDetect();
        res.add(Pair.of(rows.get(0).getTimestamp(),tsq.getConsistency()));

      }
    }
    catch (IOException ex){
      Logger.getLogger(UConsistency.class.getName()).log(Level.SEVERE,null,ex);
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate(){
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
