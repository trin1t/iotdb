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

/** This Enum class lists all supported length units in geo calculation. */
public enum LengthUnit {
  m("m"),
  km("km"),
  yd("yd"),
  mi("mi"),
  nm("nm");
  private final String unit;

  LengthUnit(String u) {
    this.unit = u;
  }
  // convert to another unit
  public double convert(LengthUnit targetUnit, double value) {
    if (targetUnit.toString().equalsIgnoreCase(this.unit)) {
      return value;
    }
    // convert to km
    switch (this.unit) {
      case "m":
        value = value / 1000.0;
        break;
      case "km":
        break;
      case "yd":
        value = value / 1093.6132983;
        break;
      case "mi":
        value = value / 0.6213712;
        break;
      case "nm":
        value = value / 0.5399568;
        break;
    }
    // convert to target unit
    switch (targetUnit) {
      case m:
        return value * 1000.0;
      case km:
        return value;
      case yd:
        return value * 1093.6132983;
      case mi:
        return value * 0.6213712;
      case nm:
        return value * 0.5399568;
    }
    return value;
  }
}
