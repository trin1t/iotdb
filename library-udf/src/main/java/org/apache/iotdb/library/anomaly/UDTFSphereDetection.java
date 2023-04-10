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

import org.apache.iotdb.library.anomaly.util.StreamSphereDetector;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

/** Detect outlier by sphere. */
public class UDTFSphereDetection implements UDTF {
  private StreamSphereDetector detector;
  private int cnt;
  private int window;
  private int step;
  private int dimension;
  private boolean onSliding;
  private ArrayList<Pair<Long, ArrayList<Double>>> points;
  private int regenerateThreshold;
  private double densityThreshold;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validate(
            params -> (int) params[0] < (int) params[1],
            "parameter $step$ (default 100) should be smaller than $window$ (default 1000).",
            validator.getParameters().getIntOrDefault("step", 100),
            validator.getParameters().getIntOrDefault("window", 1000))
        .validate(
            x -> (int) x > 0,
            "parameter $dim$ should be larger than 0.",
            validator.getParameters().getInt("dim"));
  }

  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    cnt = 0;
    dimension = udfp.getInt("dim");
    udtfc.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.BOOLEAN);
    window = udfp.getIntOrDefault("window", 100);
    step = udfp.getIntOrDefault("step", 10);
    densityThreshold = udfp.getDoubleOrDefault("density", 10d);
    regenerateThreshold = udfp.getIntOrDefault("num_in_cluster", 20);
    this.detector = new StreamSphereDetector(densityThreshold, regenerateThreshold);
    onSliding = false;
    points = new ArrayList<>();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    ArrayList<Double> coordinate = new ArrayList<>();
    for (int i = 0; i < dimension; i++) {
      coordinate.add(Util.getValueAsDouble(row, i));
    }
    cnt++;
    if (!onSliding) {
      points.add(Pair.of(row.getTime(), coordinate));
      if (cnt == window) {
        detector.initializeTree(points);
        cnt = 0;
        onSliding = true;
        points.clear();
      }
    } else {
      points.add(Pair.of(row.getTime(), coordinate));
      if (cnt == step) {
        for (Long p : detector.flush(points)) {
          collector.putBoolean(p, true);
        }
        points.clear();
        cnt = 0;
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for (Pair<Long, ArrayList<Double>> p : detector.getPossibleOutliers()) {
      collector.putBoolean(p.getLeft(), true);
    }
  }
}
