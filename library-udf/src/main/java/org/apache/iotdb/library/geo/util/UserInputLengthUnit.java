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
 * This Enum class lists all possible user input length units. They shall be converted to
 * LengthUnit.
 */
public enum UserInputLengthUnit {
  m("m"),
  meter("m"),
  meters("m"),
  metre("m"),
  metres("m"),
  km("km"),
  kilometer("km"),
  kilometers("km"),
  kilometre("km"),
  kilometres("km"),
  yd("yd"),
  yard("yd"),
  yards("yd"),
  mi("mi"),
  mile("mi"),
  miles("mi"),
  nm("nm"),
  nmi("nm"),
  nmile("nm"),
  nmiles("nm"),
  nauticalmile("nm"),
  nauticalmiles("nm");
  private final LengthUnit unit;

  UserInputLengthUnit(String u) {
    this.unit = LengthUnit.valueOf(u);
  }

  public LengthUnit getUnit() {
    return this.unit;
  }
}
