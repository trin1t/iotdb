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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.library.util.Util;
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

/** This function is used to detect density anomaly of time series. */
public class ULOF{
  private double threshold;
  private int multipleK;
  private int dim;
  private String method = "default";
  private int window;
  int windowSize = 10;

  public ArrayList<Pair<Long, Double>> getLOF(SessionDataset sds) throws Exception{
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

  int partition(Double[][] a, int left, int right) {
    Double key = a[left][1];
    Double key2 = a[left][0];
    while (left < right) {
      while (left < right && a[right][1] >= key) {
        right--;
      }
      if (left < right) {
        a[left][0] = a[right][0];
        a[left][1] = a[right][1];
      }
      while (left < right && a[left][1] <= key) {
        left++;
      }
      if (left < right) {
        a[right][0] = a[left][0];
        a[right][1] = a[left][1];
      }
    }
    a[left][0] = key2;
    a[left][1] = key;
    return left;
  }

  Double findKthNum(Double[][] a, int left, int right, int k) {
    int index = partition(a, left, right);
    if (index + 1 == k) {
      return a[index][0];
    } else if (index + 1 < k) {
      return findKthNum(a, index + 1, right, k);
    } else {
      return findKthNum(a, left, index - 1, k);
    }
  }

  public double getLOF(Double[][] knn, Double[] x, int length) {
    double sum = 0;
    for (int i = 0; i < length; i++) {
      Double[] o = knn[i];
      sum += getLocDens(knn, o, length) / getLocDens(knn, x, length);
    }
    return sum / multipleK;
  }

  public double getLocDens(Double[][] knn, Double[] x, int length) {
    Double[] nnk = findKthPoint(knn, x, length);

    double sum = 0;
    for (int i = 0; i < length; i++) {
      Double[] o = knn[i];
      sum += reachDist(o, x, nnk);
    }
    return sum / multipleK;
  }

  public Double[] findKthPoint(Double[][] knn, Double[] x, int length) {
    int index;
    double minDist = dist(knn[0], x);
    Double[][] d = new Double[length][2];
    for (int i = 0; i < length; i++) {
      d[i][0] = (double) i;
      d[i][1] = dist(knn[i], x);
    }
    index = (int) (double) (findKthNum(d, 0, length - 1, multipleK + 1));
    return knn[index];
  }

  public double reachDist(Double[] o, Double[] x, Double[] nnk) {
    return Math.max(dist(o, x), dist(nnk, x));
  }

  private double dist(Double[] nnk, Double[] x) {
    double sum = 0;
    for (int i = 0; i < nnk.length; i++) {
      sum += (nnk[i] - x[i]) * (nnk[i] - x[i]);
    }
    return Math.sqrt(sum);
  }

  public void beforeStart(){}

  public ArrayList<Pair<Long, Double>> transform(ArrayList<RowRecord> rows) throws Exception {
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();

    if (this.method.equals("default")) {
      int size = rows.size();
      Double[][] knn = new Double[size][dim];
      long[] timestamp = new long[size];
      int i = 0;
      int row = 0;
      while (row < rows.size()) {
        //timestamp[i] = rowWindow.getRow(row).getTime();
        timestamp[i] = rows.get(row).getTimestamp();
        for (int j = 0; j < dim; j++) {
          //if (!rowWindow.getRow(row).isNull(j)) {
          if (!rows.get(row).getFields().isEmpty()) {
            //knn[i][j] = Util.getValueAsDouble(rowWindow.getRow(i), j);
            knn[i][j] = rows.get(i).getFields().get(j).getDoubleV();
          } else {
            i--;
            size--;
            break;
          }
        }
        i++;
        row++;
      }
      if (size > multipleK) {
        double[] lof = new double[size];
        for (int m = 0; m < size; m++) {
          try {
            lof[m] = getLOF(knn, knn[m], size);
            res.add(Pair.of(timestamp[m], lof[m]));
          } catch (Exception e) {
            throw new Exception("Fail to get LOF " + m, e);
          }
        }
      }
    } else if (this.method.equals("series")) {
      int size = rows.size() - window + 1;
      if (size > 0) {
        Double[][] knn = new Double[size][window];
        long[] timestamp = new long[rows.size()];
        double temp;
        int i = 0;
        int row = 0;
        while (row < rows.size()) {
          //timestamp[i] = rowWindow.getRow(row).getTime();
          timestamp[i] = rows.get(row).getTimestamp();
          //if (!rowWindow.getRow(row).isNull(0)) {
          if (!rows.get(row).getFields().isEmpty()) {
            //temp = Util.getValueAsDouble(rowWindow.getRow(row), 0);
            temp = rows.get(row).getFields().get(0).getDoubleV();
            for (int p = 0; p < window; p++) {
              if (i - p < 0) {
                break;
              }
              if (i - p < size) {
                knn[i - p][p] = temp;
              }
            }
          } else {
            i--;
            size--;
          }
          i++;
          row++;
        }
        if (size > multipleK) {
          double[] lof = new double[size];
          for (int m = 0; m < size; m++) {
            try {
              lof[m] = getLOF(knn, knn[m], size);
              res.add(Pair.of(timestamp[m], lof[m]));
            } catch (Exception e) {
              throw new Exception("Fail to get LOF " + m, e);
            }
          }
        }
      }
    }
    return res;
  }

  public ArrayList<Pair<Long, Double>> terminate(){
    ArrayList<Pair<Long, Double>> res = new ArrayList<>();
    return res;
  }
}
