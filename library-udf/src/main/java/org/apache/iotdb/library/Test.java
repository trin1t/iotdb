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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.anomaly.*;
import org.apache.iotdb.library.dquality.UCompleteness;
import org.apache.iotdb.library.dquality.UConsistency;
import org.apache.iotdb.library.dquality.UTimeliness;
import org.apache.iotdb.library.dquality.UValidity;
import org.apache.iotdb.library.drepair.UTimestampRepair;
import org.apache.iotdb.library.drepair.UValueRepair;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.Session.Builder;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Describe class here.
 */
public class Test {

  public static void task(String deviceId, String measurement, String udfFunc, String udfParam, String timeLimit, TSDataType dataType, ArrayList<Double> UDFTime, ArrayList<Double> JavaTime) throws Exception {
    Session session = new Builder().build();
    session.open();
    // without UDF


    long t_start = System.currentTimeMillis();
    SessionDataSet s = session.executeQueryStatement("select " + measurement + " from root.test where time<=" + timeLimit);
//     返回类型可能并不相同
    ArrayList<Pair<Long, Double>> res = new UValueRepair().getValueRepair(s);
    for(Pair<Long, Double> r : res){
      session.insertRecord(deviceId, r.getLeft(),
              Collections.singletonList(measurement.concat("_res")),
              Collections.singletonList(dataType),
              Collections.singletonList(r.getRight()));
    }
    long t_end = System.currentTimeMillis();
    session.executeNonQueryStatement("delete timeseries " + deviceId + "." + measurement + "_res");


    String query = "select " + udfFunc + "(" + measurement + udfParam + ") into root.test(res_" + udfFunc +") from root.test where time<=" + timeLimit;
    System.out.println(query);
    long udf_start = System.currentTimeMillis();
    session.executeQueryStatement(query);
    long udf_end = System.currentTimeMillis();
    long t_time = t_end - t_start;
    long udf_time = udf_end - udf_start;
    double t = (double) t_time / 1000;
    double udf = (double) udf_time / 1000;
    System.out.println(t);
    System.out.println(udf);

    JavaTime.add(t);
    UDFTime.add(udf);
    session.close();
  }

  public static void main(String[] args)
      throws Exception {
    TSDataType dataType = TSDataType.DOUBLE;
    // 全部使用默认配置

    String deviceId = "root.test";
    String measurement = "d1";
    String udfFunc = "valuerepair";
    //String udfParams = ",\"interval\"=\"10\"";
    String udfParams = "";
    int[] pointNums = {10000,20000,50000,100000,200000,500000,1000000};
    ArrayList<Double> udfmeantime = new ArrayList<>();
    ArrayList<Double> javameantime = new ArrayList<>();
    FileOutputStream fos =  new  FileOutputStream( "res.txt" , true );
    fos.write(udfFunc.getBytes());
    fos.write("\n".getBytes());

    for(int pointNum : pointNums){
      String timeLimit = Integer.toString(pointNum * 10);
      ArrayList<Double> udftime = new ArrayList<>();
      ArrayList<Double> javatime = new ArrayList<>();
      task(deviceId, "d1", udfFunc, udfParams, timeLimit, dataType, udftime, javatime);
      task(deviceId, "d2", udfFunc, udfParams, timeLimit, dataType, udftime, javatime);
      task(deviceId, "d3", udfFunc, udfParams, timeLimit, dataType, udftime, javatime);
      double udfsum = 0d;
      for(Double t : udftime){
        udfsum += t;
      }
      udfmeantime.add(udfsum/udftime.size());
      double javasum = 0d;
      for(Double t : javatime){
        javasum += t;
      }
      javameantime.add(javasum/javatime.size());
    }

    for(Double t : udfmeantime){
      fos.write(t.toString().getBytes());
      fos.write("\t".getBytes());
    }
    fos.write("\n".getBytes());
    for(Double t : javameantime){
      fos.write(t.toString().getBytes());
      fos.write("\t".getBytes());
    }
    fos.write("\n".getBytes());

    fos.close();//记得关闭文件流
  }
}
