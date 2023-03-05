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

import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.library.dprofile.util.MADSketch;
import org.apache.iotdb.library.anomaly.util.StreamGridDetector;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * This function put data in grids (hyper-cubes) to find outliers with few neighbours.
 */
public class UDTFGridDetection implements UDTF{
  private StreamGridDetector detector;
  private int dimension;
  private int cnt;
  private int window;
  private int step;
  private boolean onSliding;
  private ArrayList<Pair<Long, ArrayList<Double>>> initialPoints;
  private long lastTimestampOfLastWindow;
  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validate(
            params -> (int) params[0] < (int) params[1],
            "parameter $step$ (default 10) should be smaller than $window$ (default 100).",
            validator.getParameters().getIntOrDefault("step", 10),
            validator.getParameters().getIntOrDefault("window", 100))
        .validate(x -> (int) x > 0, "parameter $dim$ should be larger than 0.",
            validator.getParameters().getInt("dim"));
  }

  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    cnt = 0;
    dimension = udfp.getInt("dim");
    udtfc.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.BOOLEAN);
    window = udfp.getIntOrDefault("window", 100);
    step = udfp.getIntOrDefault("step", 10);
    this.detector = new StreamGridDetector(dimension);
    onSliding = false;
    initialPoints = new ArrayList<>();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    ArrayList<Double> coordinate = new ArrayList<>();
    for(int i = 0; i < dimension; i ++){
      coordinate.add(Util.getValueAsDouble(row, i));
    }
        cnt ++;
    if(!onSliding){
      initialPoints.add(Pair.of(row.getTime(), coordinate));
      if(cnt == window){
        // todo initiate
        double[] medians = new double[dimension];
        double[] mads = new double[dimension];
        MADSketch sk;
        for(int i = 0; i < dimension; i ++){
          double[] cord = new double[window];
          sk = new MADSketch(0.01);
          for (int j = 0; j < window; j ++){
            double c = initialPoints.get(i).getRight().get(j);
            sk.insert(c);
            cord[j] = c;
          }
          medians[i] = new Median().evaluate(cord);
          mads[i] = sk.getMad().result;
        }
        detector.setGridSize(mads);
        detector.setOrigin(medians);
        for(Pair<Long, ArrayList<Double>> p : initialPoints){
          detector.insert(p.getLeft(), p.getRight());
        }
        detector.excludeInlierGrids();
        cnt = 0;
        onSliding = true;
        lastTimestampOfLastWindow = row.getTime();
      }
    }
    else{
      detector.insert(row.getTime(), coordinate);
      if(cnt == step){
        for(Long p : detector.flush(lastTimestampOfLastWindow)){
          collector.putBoolean(p, true);
        }
        lastTimestampOfLastWindow = row.getTime();
        cnt = 0;
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for(Long p : detector.terminate()){
      collector.putBoolean(p, true);
    }
  }
}
