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

package org.apache.iotdb.library.weather;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/** This function calculate the rank of the data. */
public class UDTFRank implements UDTF {
  private final ArrayList<Double> value = new ArrayList<>();
  private final ArrayList<Long> timestamp = new ArrayList<>();

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
            .validateInputSeriesNumber(1)
            .validateInputSeriesDataType(
                    0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
          throws Exception {
    configurations
            .setAccessStrategy(new RowByRowAccessStrategy())
            .setOutputDataType(TSDataType.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    long t = row.getTime();
    Double v = Util.getValueAsDouble(row);
    value.add(v);
    timestamp.add(t);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    for(int i=1; i < value.size(); i++){
      for(int j=0; j < value.size()-i; j++){
        if(value.get(j)<value.get(j+1))
        {
          double tmp=value.get(j);
          value.set(j,value.get(j+1));
          value.set(j+1,tmp);
          long tmp_t=timestamp.get(j);
          timestamp.set(j,timestamp.get(j+1));
          timestamp.set(j+1,tmp_t);
        }
      }
    }
    for (int i = 0; i < value.size(); i++) {
      collector.putDouble(timestamp.get(i), i);
    }
  }
}
