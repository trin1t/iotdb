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

/**
 * This Enum class lists all possible user input area units. They shall be converted to AreaUnit.
 */
public enum UserInputAreaUnit {
  m2("m2"),
  m_2("m2"),
  squaremeter("m2"),
  square_meter("m2"),
  squaremeters("m2"),
  square_meters("m2"),
  squaremetre("m2"),
  square_metre("m2"),
  squaremetres("m2"),
  square_metres("m2"),
  sqm("m2"),
  km2("km2"),
  km_2("km2"),
  squarekilometer("km2"),
  square_kilometer("km2"),
  squarekilometers("km2"),
  square_kilometers("km2"),
  squarekilometre("km2"),
  square_kilometre("km2"),
  squarekilometres("km2"),
  square_kilometres("km2"),
  sqkm("km2"),
  sqyd("sqyd"),
  squareyard("sqyd"),
  square_yard("sqyd"),
  squareyards("sqyd"),
  square_yards("sqyd"),
  yd2("sqyd"),
  yd_2("sqyd"),
  sqmi("sqmi"),
  squaremile("sqmi"),
  square_mile("sqmi"),
  squaremiles("sqmi"),
  square_miles("sqmi"),
  mi2("sqmi"),
  mi_2("sqmi"),
  ac("acre"),
  acre("acre"),
  acres("acre");
  private final AreaUnit unit;

  UserInputAreaUnit(String u) {
    this.unit = AreaUnit.valueOf(u);
  }

  public AreaUnit getUnit() {
    return this.unit;
  }
}
