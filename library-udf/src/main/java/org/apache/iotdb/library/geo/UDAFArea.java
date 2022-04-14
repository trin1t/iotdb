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
import org.apache.iotdb.library.geo.util.AreaUnit;
import org.apache.iotdb.library.geo.util.UserInputAreaUnit;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.EnumUtils;

import java.util.ArrayList;
import java.util.Locale;

/** This function calculates the area of a polygon with GPS positions. */
public class UDAFArea implements UDTF {
  private ArrayList<Double> longtitude;
  private ArrayList<Double> latitude;
  private AreaUnit unit;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(2)
        .validateInputSeriesDataType(0, TSDataType.DOUBLE, TSDataType.FLOAT)
        .validateInputSeriesDataType(1, TSDataType.DOUBLE, TSDataType.FLOAT)
        .validate(
            x -> EnumUtils.isValidEnumIgnoreCase(UserInputAreaUnit.class, (String) x),
            "Parameter $unit$ should be within {m2, km2, sqmi, sqft, acre}.",
            validator.getParameters().getStringOrDefault("unit", "km2"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.DOUBLE);
    longtitude = new ArrayList<>();
    latitude = new ArrayList<>();
    unit =
        UserInputAreaUnit.valueOf(
                parameters.getStringOrDefault("unit", "km2").toLowerCase(Locale.ROOT))
            .getUnit();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0)
        && Double.isFinite(Util.getValueAsDouble(row, 0))
        && !row.isNull(1)
        && Double.isFinite(Util.getValueAsDouble(row, 1))) {
      double lat = Util.getValueAsDouble(row, 0);
      double lon = Util.getValueAsDouble(row, 1);
      if (lat >= -180.0 && lat <= 180.0 && lon >= -90.0 && lon <= 90.0) {
        latitude.add(lat);
        longtitude.add(lon);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    if (latitude.size() >= 3) {
      double area = area(latitude, longtitude);
      area = AreaUnit.km2.convert(unit, area);
      collector.putDouble(0, area);
    }
  }

  private double area(ArrayList<Double> lats, ArrayList<Double> lons) {
    final double surfaceArea = 5.10072E8;
    double sum = 0;
    double prevcolat = 0;
    double prevaz = 0;
    double colat0 = 0;
    double az0 = 0;
    for (int i = 0; i < lats.size(); i++) {
      double colat =
          2
              * Math.atan2(
                  Math.sqrt(
                      Math.pow(Math.sin(lats.get(i) * Math.PI / 180 / 2), 2)
                          + Math.cos(lats.get(i) * Math.PI / 180)
                              * Math.pow(Math.sin(lons.get(i) * Math.PI / 180 / 2), 2)),
                  Math.sqrt(
                      1
                          - Math.pow(Math.sin(lats.get(i) * Math.PI / 180 / 2), 2)
                          - Math.cos(lats.get(i) * Math.PI / 180)
                              * Math.pow(Math.sin(lons.get(i) * Math.PI / 180 / 2), 2)));
      double az = 0;
      az =
          Math.atan2(
                  Math.cos(lats.get(i) * Math.PI / 180) * Math.sin(lons.get(i) * Math.PI / 180),
                  Math.sin(lats.get(i) * Math.PI / 180))
              % (2 * Math.PI);
      if (i == 0) {
        colat0 = colat;
        az0 = az;
      }
      if (i > 0) {
        double v = Math.abs(az - prevaz) / Math.PI;
        sum =
            sum
                + (1 - Math.cos(prevcolat + (colat - prevcolat) / 2))
                    * Math.PI
                    * (v - 2 * Math.ceil((v - 1) / 2))
                    * Math.signum(az - prevaz);
      }
      prevcolat = colat;
      prevaz = az;
    }
    sum = sum + (1 - Math.cos(prevcolat + (colat0 - prevcolat) / 2)) * (az0 - prevaz);
    return surfaceArea * Math.min(Math.abs(sum) / 4 / Math.PI, 1 - Math.abs(sum) / 4 / Math.PI);
  }
}
