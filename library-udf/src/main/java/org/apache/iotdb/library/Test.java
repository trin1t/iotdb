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
package org.apache.iotdb.library;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.dquality.UCompleteness;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.Session.Builder;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Describe class here.
 */
public class Test {

  public static void main(String[] args)
      throws Exception {
    String deviceId = "root.test";
    List<String> measurement = Collections.singletonList("r1");
    TSDataType dataType = TSDataType.DOUBLE;
    // 全部使用默认配置
    Session session = new Builder().build();
    session.open();

    // without UDF
    long t_start = System.currentTimeMillis();
    SessionDataSet s = session.executeQueryStatement("select * from root.test");
    // 返回类型可能并不相同
    ArrayList<Pair<Long, Double>> res = new UCompleteness().getCompleteness(s);
    for(Pair<Long, Double> r : res){
      session.insertRecord(deviceId, r.getLeft(), measurement, Collections.singletonList(dataType) ,
          Collections.singletonList(r.getRight()));
    }
    long t_end = System.currentTimeMillis();

    long udf_start = System.currentTimeMillis();
    session.executeNonQueryStatement("select ... into root.test from ... where ...");
    long udf_end = System.currentTimeMillis();

    System.out.print(t_end - t_start);
    System.out.print("\t");
    System.out.print(udf_end - udf_start);
    System.out.println("\n");
    session.close();
  }
}
