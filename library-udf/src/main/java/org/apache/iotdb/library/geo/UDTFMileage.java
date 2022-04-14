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

package org.apache.iotdb.library.geo;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.geo.util.Distance;
import org.apache.iotdb.library.geo.util.LengthUnit;
import org.apache.iotdb.library.geo.util.UserInputLengthUnit;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.EnumUtils;

import java.util.Locale;

/** This function calculates accumulated mileage on each GPS position, using spherical distance. */
public class UDTFMileage implements UDTF {

  private double lat0, long0, lat1, long1; // records two sets of GPS position
  private double accumulatedMileage; // records accumulated mileage
  private boolean isFirstPoint;

  private LengthUnit unit;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, TSDataType.DOUBLE, TSDataType.FLOAT)
        .validateInputSeriesDataType(1, TSDataType.DOUBLE, TSDataType.FLOAT)
        .validate(
            x -> EnumUtils.isValidEnumIgnoreCase(UserInputLengthUnit.class, (String) x),
            "Parameter $unit$ should be within {m, km, ft, mi, nm}.",
            validator.getParameters().getIntOrDefault("lag", 0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    accumulatedMileage = 0;
    isFirstPoint = true;
    unit =
        UserInputLengthUnit.valueOf(
                parameters.getStringOrDefault("unit", "km").toLowerCase(Locale.ROOT))
            .getUnit();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0)
        && Double.isFinite(Util.getValueAsDouble(row, 0))
        && !row.isNull(1)
        && Double.isFinite(Util.getValueAsDouble(row, 1))) {
      // check if input GPS is legal
      lat1 = Util.getValueAsDouble(row, 0);
      long1 = Util.getValueAsDouble(row, 1);
      if (lat1 >= -180.0 && lat1 <= 180.0 && long1 >= -90.0 && long1 <= 90.0) {
        if (isFirstPoint) {
          lat0 = lat1;
          long0 = long1;
          isFirstPoint = false;
          collector.putDouble(row.getTime(), 0);
        } else {
          accumulatedMileage +=
              LengthUnit.km.convert(unit, Distance.distance(lat0, long0, lat1, long1));
          lat0 = lat1;
          long0 = long1;
          collector.putDouble(row.getTime(), accumulatedMileage);
        }
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {}
}
