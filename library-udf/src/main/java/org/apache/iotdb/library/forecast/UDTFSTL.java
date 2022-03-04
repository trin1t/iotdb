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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess;
import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess.Builder;

import java.util.ArrayList;

/** Seasonal Trend decomposition procedure based on Loess */
public class UDTFSTL implements UDTF {
  private ArrayList<Long> timestamp = new ArrayList<>();
  private ArrayList<Double> value = new ArrayList<>();
  private int period;
  private int sWindow;
  private int sDegree;
  private int tWindow;
  private int tDegree;
  private int lWindow;
  private int lDegree;
  private int sJump;
  private int tJump;
  private int lJump;
  private boolean robust;
  private String output;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validate(
            period -> (int) period > 1,
            "\"period\" should be an integer greater than one.",
            validator.getParameters().getInt("period"))
        .validate(
            swindow -> ((int) swindow + 2) % 2 == 1 && ((int) swindow >= 7 || (int) swindow == -1),
            "\"sWindow\" should be odd and at least 7, or leave it empty for default \" periodic\".",
            validator.getParameters().getIntOrDefault("swindow", -1))
        .validate(
            sdegree -> (int) sdegree == 0 || (int) sdegree == 1,
            "\"sDegree\" should be zero or one.",
            validator.getParameters().getIntOrDefault("sdegree", 0))
        .validate(
            twindow -> ((int) twindow + 2) % 2 == 1 && (int) twindow >= -1,
            "\"tWindow\" should be odd and positive.",
            validator.getParameters().getIntOrDefault("twindow", -1))
        .validate(
            tdegree -> (int) tdegree == 0 || (int) tdegree == 1,
            "\"tDegree\" should be zero or one.",
            validator.getParameters().getIntOrDefault("tdegree", 1))
        .validate(
            lwindow -> ((int) lwindow + 2) % 2 == 1 && (int) lwindow >= -1,
            "\"lWindow\" should be odd and positive.",
            validator.getParameters().getIntOrDefault("lwindow", -1))
        .validate(
            ldegree -> (int) ldegree == 0 || (int) ldegree == 1 || (int) ldegree == -1,
            "\"lDegree\" should be zero or one.",
            validator.getParameters().getIntOrDefault("ldegree", -1))
        .validate(
            sjump -> (int) sjump >= 0,
            "\"sJump\" should be positive.",
            validator.getParameters().getIntOrDefault("sjump", 0))
        .validate(
            tjump -> (int) tjump >= 0,
            "\"tJump\" should be positive.",
            validator.getParameters().getIntOrDefault("tjump", 0))
        .validate(
            ljump -> (int) ljump >= 0,
            "\"lJump\" should be positive.",
            validator.getParameters().getIntOrDefault("ljump", 0))
        .validate(
            output ->
                ((String) output).equalsIgnoreCase("trend")
                    || ((String) output).equalsIgnoreCase("seasonal")
                    || ((String) output).equalsIgnoreCase("residual"),
            "\"output\" should be \"trend\", \"seasonal\" or \"residual\".",
            validator.getParameters().getStringOrDefault("output", "trend"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    timestamp.clear();
    value.clear();
    period = parameters.getInt("period");
    sWindow = parameters.getIntOrDefault("swindow", -1);
    sDegree = parameters.getIntOrDefault("sdegree", 0);
    tWindow = parameters.getIntOrDefault("twindow", -1);
    tDegree = parameters.getIntOrDefault("tdegree", 1);
    lWindow = parameters.getIntOrDefault("lwindow", -1);
    lDegree = parameters.getIntOrDefault("ldegree", tDegree);
    sJump = parameters.getIntOrDefault("sjump", 0);
    tJump = parameters.getIntOrDefault("tjump", 0);
    lJump = parameters.getIntOrDefault("ljump", 0);
    robust = parameters.getBooleanOrDefault("robust", false);
    output = parameters.getStringOrDefault("output", "trend");
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    Double v = Util.getValueAsDouble(row);
    if (!v.isNaN()) {
      timestamp.add(row.getTime());
      value.add(v);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    double[] values = value.stream().mapToDouble(Double::valueOf).toArray();
    SeasonalTrendLoess.Builder builder = new Builder();
    builder.setPeriodLength(period);
    if (sWindow > 0) {
      builder.setSeasonalWidth(sWindow);
    } else {
      builder.setPeriodic();
    }
    builder.setSeasonalDegree(sDegree);
    if (tWindow > 0) {
      builder.setTrendWidth(tWindow);
    }
    builder.setTrendDegree(tDegree);
    if (lWindow > 0) {
      builder.setLowpassWidth(lWindow);
    }
    builder.setLowpassDegree(lDegree);
    if (sJump > 0) {
      builder.setSeasonalJump(sJump);
    }
    if (tJump > 0) {
      builder.setTrendJump(tJump);
    }
    if (lJump > 0) {
      builder.setLowpassJump(lJump);
    }
    if (robust) {
      builder.setRobust();
    } else {
      builder.setNonRobust();
    }
    SeasonalTrendLoess smoother = builder.buildSmoother(values);
    SeasonalTrendLoess.Decomposition stl = smoother.decompose();
    if (output.equalsIgnoreCase("trend")) {
      double[] trend = stl.getTrend();
      for (int i = 0; i < trend.length; i++) {
        collector.putDouble(timestamp.get(i + timestamp.size() - trend.length), trend[i]);
      }
    } else if (output.equalsIgnoreCase("seasonal")) {
      double[] seasonal = stl.getSeasonal();
      for (int i = 0; i < seasonal.length; i++) {
        collector.putDouble(timestamp.get(i + timestamp.size() - seasonal.length), seasonal[i]);
      }
    } else if (output.equalsIgnoreCase("residual")) {
      double[] residual = stl.getResidual();
      for (int i = 0; i < residual.length; i++) {
        collector.putDouble(timestamp.get(i + timestamp.size() - residual.length), residual[i]);
      }
    }
  }
}
