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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.dprofile.util.ExactOrderStatistics;
import org.apache.iotdb.library.dprofile.util.GKArray;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;

/** calculate the approximate percentile */
public class UDAFPercentile implements UDTF {
  public static HashMap<Double, Long> Dic;
  private ExactOrderStatistics statistics;
  private GKArray sketch;
  private boolean exact;
  private double rank;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validate(
            error -> (double) error >= 0 && (double) error < 1,
            "error has to be greater than or equal to 0 and less than 1.",
            validator.getParameters().getDoubleOrDefault("error", 0))
        .validate(
            rank -> (double) rank > 0 && (double) rank <= 1,
            "rank has to be greater than 0 and less than or equal to 1.",
            validator.getParameters().getDoubleOrDefault("rank", 0.5));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    double error = parameters.getDoubleOrDefault("error", 0);
    rank = parameters.getDoubleOrDefault("rank", 0.5);
    exact = (error == 0);
    if (exact) {
      statistics = new ExactOrderStatistics(parameters.getDataType(0));
    } else {
      sketch = new GKArray(error);
      Dic = new HashMap<>();
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (exact) {
      statistics.insert(row);
    } else {
      double res = Util.getValueAsDouble(row);
      sketch.insert(res);
      Long time = row.getTime();
      Dic.put(res, time);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (exact) {
      double res = statistics.getPercentile(rank);
      long time = Dic.get(res);
      collector.putDouble(time, res);
    } else {
      double res = sketch.query(rank);
      collector.putDouble(0, res);
    }
  }
}
