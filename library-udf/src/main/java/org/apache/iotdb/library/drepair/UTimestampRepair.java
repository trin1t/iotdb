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

import javafx.util.Pair;
import org.apache.iotdb.library.drepair.util.TimestampRepair;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;

/** This function is used for timestamp repair. */
public class UTimestampRepair{
  int intervalMode;
  int windowSize = 10;

  public ArrayList<Pair<Long, Double>> getTimestampRepair(SessionDataset sds) throws Exception{
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

  public void beforeStart() {}

  public ArrayList<Pair<Long, Double>> transform(ArrayList<RowRecord> rows) throws Exception{
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    TimestampRepair ts = new TimestampRepair(rows, intervalMode, 2);
    ts.dpRepair();
    long[] timestamp = ts.getRepaired();
    double[] value = ts.getRepairedValue();
    switch (rows.getType(0)) {
      case DOUBLE:
        for (int i = 0; i < timestamp.length; i++) {
          res.add(Pair.of(timestamp[i],value[i]));
        }
        break;
      case FLOAT:
        for (int i = 0; i < timestamp.length; i++) {
          res.add(Pair.of(timestamp[i], (float) value[i]));
        }
        break;
      case INT32:
        for (int i = 0; i < timestamp.length; i++) {
          res.add(Pair.of(timestamp[i], (int) value[i]));
        }
        break;
      case INT64:
        for (int i = 0; i < timestamp.length; i++) {
          res.add(Pair.of(timestamp[i], (long) value[i]));
        }
        break;
      default:
        throw new Exception();
    }
  }

  public ArrayList<Pair<Long, Double>> terminate(){
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
