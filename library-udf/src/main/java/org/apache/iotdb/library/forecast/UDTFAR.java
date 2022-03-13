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

package org.apache.iotdb.library.forecast;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.library.util.Autoregressive;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;


/** AR forecast for time series */
public class UDTFAR implements UDTF {
    private ArrayList<Double> value = new ArrayList<>();
    private int p;
    private int steps;
    private String output;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
      validator
          .validateInputSeriesNumber(1)
          .validateInputSeriesDataType(
              0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
          .validate(
              p -> (int) p > 0,
              "\"p\" must be a positive integer.",
              validator.getParameters().getInt("p"))
          .validate(
              steps -> (int) steps > 0,
              "\"steps\" must be a positive integer.",
              validator.getParameters().getIntOrDefault("steps", 1))
          .validate(
              output ->
                  ((String) output).equalsIgnoreCase("coefficient")
                      || ((String) output).equalsIgnoreCase("sequence")
                      || ((String) output).equalsIgnoreCase("forecast")
                      || ((String) output).equalsIgnoreCase("residual"),
              "\"output\" should be \"coefficient\", \"sequence\", \"forecast\", or \"residual\"",
              validator.getParameters().getStringOrDefault("output", "coefficient"));
    }

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
        throws Exception {
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.DOUBLE);
      value.clear();
      p = parameters.getInt("p");
      steps = parameters.getIntOrDefault("steps", 1);
      output = parameters.getStringOrDefault("output", "coefficient");
    }
  
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
      double v = Util.getValueAsDouble(row);
      if (!Double.isNaN(v)) {
        value.add(v);
      }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {

        double[] originalData = new double[this.value.size()];
        for (int i = 0; i < this.value.size(); i ++) {
            originalData[i] = this.value.get(i);
        }
        Autoregressive ar = new Autoregressive(originalData, this.p);
        if (output.equalsIgnoreCase("coefficient")) {
            double[] coefficient = ar.getCoefficient();
            for (int i = 0; i < coefficient.length; i++) {
                collector.putDouble(i, coefficient[i]);
            }
        } else if (output.equalsIgnoreCase("sequence")) {
            double[] sequence = ar.getFitSeq();
            for (int i = 0; i < sequence.length; i++) {
                collector.putDouble(i, sequence[i]);
            }
        }  else if (output.equalsIgnoreCase("forecast")) {
            double[] forecastSeq = ar.forecast(steps);
            for (int i = 0; i < forecastSeq.length; i++) {
                collector.putDouble(i, forecastSeq[i]);
            }
        }  else if (output.equalsIgnoreCase("sequence")) {
            double[] sequence = ar.getFitSeq();
            for (int i = 0; i < sequence.length; i++) {
                collector.putDouble(i, sequence[i]);
            }
        } else if (output.equalsIgnoreCase("residual")) {
            double[] residual = ar.getResidual();
            for (int i = 0; i < residual.length; i++) {
                collector.putDouble(i, residual[i]);
            }
        }
    }
}
  