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

import org.apache.iotdb.library.dquality.util.StreamTimeSeriesQuality;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/** This function calculates completeness of input series. */
public class UDTFStreamConsistency implements UDTF {
  private StreamTimeSeriesQuality stsq;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesDataType(0, Type.DOUBLE, Type.FLOAT, Type.INT32, Type.INT64)
        .validate(
            params -> (int) params[0] < (int) params[1],
            "parameter $step$ (default 10) should be smaller than $window$ (default 100).",
            validator.getParameters().getIntOrDefault("step", 10),
            validator.getParameters().getIntOrDefault("window", 100))
        .validate(
            x -> (int) x > 0,
            "parameter $interval$ should be larger than 0.",
            validator.getParameters().getIntOrDefault("interval", 1));
  }

  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    udtfc.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    int window = udfp.getIntOrDefault("window", 100);
    int step = udfp.getIntOrDefault("step", 10);
    int interval = udfp.getIntOrDefault("interval", -1);
    stsq = new StreamTimeSeriesQuality("consistency", window, step, interval);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    stsq.updateStreamTimeSeriesQuality(row);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    collector.putDouble(0, stsq.getDataQuality());
  }
}
