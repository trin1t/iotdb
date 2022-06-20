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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class WeatherTests {
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
    IoTDB.schemaProcessor.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.weather.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.weather.d1.s2"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.weather.d1.s3"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.weather.d2.s1"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.schemaProcessor.createTimeseries(
        new PartialPath("root.weather.d2.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          String.format(
              "insert into root.weather.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%d))",
              1651852800, 100, 100, 90));
      statement.addBatch(
          String.format(
              "insert into root.weather.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%d))",
              1651852900, 105, 105, 100));
      statement.addBatch(
          String.format(
              "insert into root.weather.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%d))",
              1651853000, 95, 95, 80));
      statement.addBatch(
          String.format(
              "insert into root.weather.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%d))",
              1651853100, 90, 90, 100));
      statement.addBatch(
          String.format(
              "insert into root.weather.d1(timestamp,s1,s2,s3) values(%d,%d,%d,%d))",
              1651853200, 100, 100, 100));
      statement.addBatch(
          String.format(
              "insert into root.weather.d2(timestamp,s1,s2) values(%d,%d,%d))",
              1651852800, 10, 15));
      statement.addBatch(
          String.format(
              "insert into root.weather.d2(timestamp,s1,s2) values(%d,%d,%d))",
              1651852900, 15, 20));
      statement.addBatch(
          String.format(
              "insert into root.weather.d2(timestamp,s1,s2) values(%d,%d,%d))", 1651853000, 5, 0));
      statement.addBatch(
          String.format(
              "insert into root.weather.d2(timestamp,s1,s2) values(%d,%d,%d))", 1651853100, 0, 5));
      statement.addBatch(
          String.format(
              "insert into root.weather.d2(timestamp,s1,s2) values(%d,%d,%d))",
              1651853200, 20, 10));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function acc as 'org.apache.iotdb.library.weather.UDAFACC'");
      statement.execute("create function cusum as 'org.apache.iotdb.library.weather.UDTFCUSUM'");
      statement.execute("create function rank as 'org.apache.iotdb.library.string.UDTFRank'");
      statement.execute(
          "create function EuclidDis as 'org.apache.iotdb.library.string.UDAFEuclidDis'");
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
  public void testCov1() {
    String sqlStr = "select acc(d1.s1,d1.s2,d1.s3) from root.weather";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(1);
      Assert.assertEquals(1.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCusum1() {
    String sqlStr = "select cusum(d2.s1) from root.weather";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(1);
      Assert.assertEquals(10, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(1);
      Assert.assertEquals(25, result2, 0.01);
      resultSet.next();
      double result3 = resultSet.getDouble(1);
      Assert.assertEquals(30, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(1);
      Assert.assertEquals(30, result4, 0.01);
      resultSet.next();
      double result5 = resultSet.getDouble(1);
      Assert.assertEquals(50, result5, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRank1() {
    String sqlStr = "select rank(d2.s1) from root.weather";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(1);
      Assert.assertEquals(3, result1, 0.01);
      resultSet.next();
      double result2 = resultSet.getDouble(1);
      Assert.assertEquals(4, result2, 0.01);
      resultSet.next();
      double result3 = resultSet.getDouble(1);
      Assert.assertEquals(2, result3, 0.01);
      resultSet.next();
      double result4 = resultSet.getDouble(1);
      Assert.assertEquals(1, result4, 0.01);
      resultSet.next();
      double result5 = resultSet.getDouble(1);
      Assert.assertEquals(5, result5, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testEuclidDis1() {
    String sqlStr = "select eucliddis(d2.s1,d2.s2) from root.weather";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(1);
      Assert.assertEquals(14.14, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
