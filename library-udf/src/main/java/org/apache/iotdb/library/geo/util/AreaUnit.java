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
package org.apache.iotdb.library.geo.util;

/** This Enum class lists all supported area units in geo calculation. */
public enum AreaUnit {
  m2("m2"),
  km2("km2"),
  acre("acre"),
  sqmi("sqmi"),
  sqyd("sqyd");
  private final String unit;

  AreaUnit(String u) {
    this.unit = u;
  }
  // convert to another unit
  public double convert(AreaUnit targetUnit, double value) {
    if (targetUnit.toString().equalsIgnoreCase(this.unit)) {
      return value;
    }
    // convert to square kilometer
    switch (this.unit) {
      case "m2":
        value = value / 1000000.0;
        break;
      case "km2":
        break;
      case "sqyd":
        value = value / 1195990.046301;
        break;
      case "sqmi":
        value = value / 0.3861022;
        break;
      case "acre":
        value = value / 247.1053815;
        break;
    }
    // convert to target unit
    switch (targetUnit) {
      case m2:
        return value * 1000000.0;
      case km2:
        return value;
      case sqyd:
        return value * 1195990.046301;
      case sqmi:
        return value * 0.3861022;
      case acre:
        return value * 247.1053815;
    }
    return value;
  }
}
