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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class GeoTests {
  protected static final int ITERATION_TIMES = 16384;
  protected static final int DELTA_T = 100;

  private static final float oldUdfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();
  private static final float oldUdfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  private static final float oldUdfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.schemaEngine.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.schemaEngine.createTimeseries(
        new PartialPath("root.vehicle.d1.lat"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaEngine.createTimeseries(
        new PartialPath("root.vehicle.d1.lon"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,lat,lon) values(%d, 116.8, 39.7)", DELTA_T));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,lat,lon) values(%d, 116.5, 40.0)",
              2 * DELTA_T));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,lat,lon) values(%d, 116.2, 40.1)",
              3 * DELTA_T));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,lat,lon) values(%d, 115.9, 39.9)",
              4 * DELTA_T));
      statement.execute(
          String.format(
              "insert into root.vehicle.d1(timestamp,lat,lon) values(%d, 116.2, 39.8)",
              5 * DELTA_T));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function area as 'org.apache.iotdb.library.geo.UDAFArea'");
      statement.execute("create function deconv as 'org.apache.iotdb.library.geo.UDTFMileage'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(oldUdfCollectorMemoryBudgetInMB)
        .setUdfTransformerMemoryBudgetInMB(oldUdfTransformerMemoryBudgetInMB)
        .setUdfReaderMemoryBudgetInMB(oldUdfReaderMemoryBudgetInMB);
  }

  @Test
  public void testArea1() {
    String sqlStr = "select area(d1.lat, d1.lon, 'unit'='km2') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      double result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 905.0, 1.0);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMileage1() {
    String sqlStr = "select mileage(d1.lat, d1.lon, 'unit'='km') from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      double result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 0.0, 0.1);
      resultSet.next();
      result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 42.06, 0.1);
      resultSet.next();
      result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 69.91, 0.1);
      resultSet.next();
      result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 103.79, 0.1);
      resultSet.next();
      result = Double.parseDouble(resultSet.getString(1));
      Assert.assertEquals(result, 131.71, 0.1);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
