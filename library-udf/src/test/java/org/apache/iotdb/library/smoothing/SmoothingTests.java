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

package org.apache.iotdb.library.smoothing;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
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

public class SmoothingTests {
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
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.season"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.season.d1.s1"), // period=12
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    double[] datad1 =
        new double[] {
          315.71, 317.45, 317.5, 317.1, 315.86, 314.93, 313.2, 312.66, 313.33, 314.67, 315.62,
          316.38, 316.71, 317.72, 318.29, 318.15, 316.54, 314.8, 313.84, 313.26, 314.8, 315.58,
          316.43, 316.97, 317.58, 319.02, 320.03, 319.59, 318.18, 315.91, 314.16, 313.83, 315,
          316.19, 316.93, 317.7, 318.54, 319.48, 320.58, 319.77, 318.57, 316.79, 314.8, 315.38,
          316.1, 317.01, 317.94, 318.56, 319.68, 320.63, 321.01, 320.55, 319.58, 317.4, 316.26,
          315.42, 316.69, 317.69, 318.74, 319.08, 319.86, 321.39, 322.25, 321.47, 319.74, 317.77,
          316.21, 315.99, 317.12, 318.31, 319.57, 320.07, 320.73, 321.77, 322.25, 321.89, 320.44,
          318.7, 316.7, 316.79, 317.79, 318.71, 319.44, 320.44, 320.89, 322.13, 322.16, 321.87,
          321.39, 318.81, 317.81, 317.3, 318.87, 319.42, 320.62, 321.59, 322.39, 323.87, 324.01,
          323.75, 322.39, 320.37, 318.64, 318.1, 319.79, 321.08, 322.07, 322.5, 323.04, 324.42, 325,
          324.09, 322.55, 320.92, 319.31, 319.31, 320.72, 321.96, 322.57, 323.15, 323.89, 325.02,
          325.57, 325.36, 324.14, 322.03, 320.41, 320.25, 321.31, 322.84, 324, 324.42, 325.64,
          326.66, 327.34, 326.76, 325.88, 323.67, 322.38, 321.78, 322.85, 324.11, 325.03, 325.99,
          326.87, 328.13, 328.07, 327.66, 326.35, 324.69, 323.1, 323.16, 323.98, 325.13, 326.17,
          326.68, 327.18, 327.78, 328.92, 328.57, 327.34, 325.46, 323.36, 323.57, 324.8, 326.01,
          326.77, 327.63, 327.75, 329.72, 330.07, 329.09, 328.05, 326.32, 324.93, 325.06, 326.5,
          327.55, 328.54, 329.56, 330.3, 331.5, 332.48, 332.07, 330.87, 329.31, 327.51, 327.18,
          328.16, 328.64, 329.35, 330.71, 331.48, 332.65, 333.2, 332.16, 331.07, 329.12, 327.32,
          327.28, 328.3, 329.58, 330.73, 331.46, 331.9, 333.17, 333.94, 333.45, 331.98, 329.95,
          328.5, 328.35, 329.37, 330.58, 331.59, 332.75, 333.52, 334.64, 334.77, 334, 333.06,
          330.68, 328.95, 328.75, 330.15, 331.62, 332.66, 333.13, 334.95, 336.13, 336.93, 336.16,
          334.88, 332.56, 331.29, 331.27, 332.41, 333.6, 334.95, 335.25, 336.66, 337.69, 338.03,
          338.01, 336.41, 334.41, 332.37, 332.41, 333.75, 334.9, 336.14, 336.69, 338.27, 338.96,
          339.21, 339.26, 337.54, 335.75, 333.98, 334.19, 335.31, 336.81, 337.9, 338.34, 340.01,
          340.93, 341.48, 341.33, 339.4, 337.7, 336.19, 336.15, 337.27, 338.32, 339.29, 340.55,
          341.61, 342.53, 343.03, 342.54, 340.78, 338.44, 336.95, 337.08, 338.58, 339.88, 340.96,
          341.73, 342.81, 343.97, 344.63, 343.79, 342.32, 340.09, 338.28, 338.29, 339.6, 340.9,
          341.68, 342.9, 343.33, 345.25, 346.03, 345.63, 344.19, 342.27, 340.35, 340.38, 341.59,
          343.05, 344.1, 344.79, 345.52, 346.84, 347.63, 346.97, 345.53, 343.55, 341.4, 341.67,
          343.1, 344.7, 345.21, 346.16, 347.74, 348.33, 349.06, 348.38, 346.72, 345.02, 343.27,
          343.13, 344.49, 345.88, 346.56, 347.28, 348.01, 349.77, 350.38, 349.93, 348.16, 346.08,
          345.22, 344.51, 345.93, 347.22, 348.52, 348.73, 349.73, 351.31, 352.09, 351.53, 350.11,
          348.08, 346.52, 346.59, 347.96, 349.16, 350.39, 351.64, 352.41, 353.69, 354.21, 353.72,
          352.69, 350.4, 348.92, 349.13, 350.2, 351.41, 352.91, 353.27, 353.96, 355.64, 355.86,
          355.37, 353.99, 351.81, 350.05, 350.25, 351.49, 352.85, 353.8, 355.04, 355.73, 356.32,
          357.32, 356.34, 354.84, 353.01, 351.31, 351.62, 353.07, 354.33, 354.84, 355.73, 357.23,
          358.66, 359.13, 358.13, 356.19, 353.85, 352.25, 352.35, 353.81, 355.12, 356.25, 357.11,
          357.86, 359.09, 359.59, 359.33, 357.01, 354.94, 352.95, 353.32, 354.32, 355.57, 357,
          357.31, 358.47, 359.27, 360.19, 359.52, 357.33, 355.64, 354.03, 354.12, 355.41, 356.91,
          358.24, 358.92, 359.99, 361.23, 361.65, 360.81, 359.38, 357.46, 355.73, 356.07, 357.53,
          358.98, 359.92, 360.86, 361.83, 363.3, 363.69, 363.19, 361.64, 359.12, 358.17, 357.99,
          359.45, 360.68, 362.07, 363.24, 364.17, 364.57, 365.13, 364.92, 363.55, 361.38, 359.54,
          359.58, 360.89, 362.24, 363.09, 364.03, 364.51, 366.35, 366.64, 365.59, 364.31, 362.25,
          360.29, 360.82, 362.49, 364.38, 365.26, 365.98, 367.24, 368.66, 369.42, 368.99, 367.82,
          365.95, 364.02, 364.4, 365.52, 367.13, 368.18, 369.07, 369.68, 370.99, 370.96, 370.3,
          369.45, 366.9, 364.81, 365.37, 366.72, 368.1, 369.29, 369.54, 370.6, 371.81, 371.58,
          371.7, 369.86, 368.13, 367, 367.03, 368.37, 369.67, 370.59, 371.51, 372.43, 373.37,
          373.85, 373.21, 371.51, 369.61, 368.18, 368.45, 369.76, 371.24, 372.53, 373.2, 374.12,
          375.02, 375.76, 375.52, 374.01, 371.85, 370.75, 370.55, 372.25, 373.79, 374.88, 375.64,
          376.45, 377.73, 378.6, 378.28, 376.7, 374.38, 373.17, 373.14, 374.66, 375.99, 377, 377.87,
          378.88, 380.35, 380.62, 379.69, 377.47, 376.01, 374.25, 374.46, 376.16, 377.51, 378.46,
          379.73, 380.77, 382.29, 382.45, 382.22, 380.74, 378.74, 376.7, 377, 378.35, 380.11,
          381.38, 382.19, 382.67, 384.61, 385.03, 384.05, 382.46, 380.41, 378.85, 379.13, 380.15,
          381.82, 382.89, 383.9, 384.58, 386.5, 386.56, 386.1, 384.5, 381.99, 380.96, 381.12,
          382.45, 383.95, 385.52, 385.82, 386.03, 387.21, 388.54, 387.76, 386.36, 384.09, 383.18,
          382.99, 384.19, 385.56, 386.94, 387.48, 388.82, 389.55, 390.14, 389.48, 388.03, 386.11,
          384.74, 384.43, 386.02, 387.42, 388.71, 390.2, 391.17, 392.46, 393, 392.15, 390.2, 388.35,
          386.85, 387.24, 388.67, 389.79, 391.33, 391.86, 392.6, 393.25, 394.19, 393.73, 392.51,
          390.13, 389.08, 389, 390.28, 391.86, 393.12, 393.86, 394.4, 396.18, 396.74, 395.71,
          394.36, 392.39, 391.11, 391.05, 392.98, 394.34, 395.55, 396.8, 397.43, 398.41, 399.78,
          398.61, 397.32, 395.2, 393.45, 393.7, 395.16, 396.84, 397.85, 398.01, 399.77, 401.38,
          401.78, 401.25, 399.1, 397.03, 395.38, 396.03, 397.28, 398.91, 399.98, 400.28, 401.54,
          403.28, 403.96, 402.8, 401.31, 398.93, 397.63, 398.29, 400.16, 401.85, 402.52, 404.04,
          404.83, 407.42, 407.7, 406.81, 404.39, 402.25, 401.03, 401.57, 403.53, 404.48, 406.13,
          406.42
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long ts = 1;
      for (double d : datad1) {
        statement.addBatch(
            String.format("insert into root.season.d1(timestamp,s1) values(%d,%f)", ts, d));
        ts++;
      }
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function ema as 'org.apache.iotdb.library.smoothing.UDTFDEMA'");
      statement.execute("create function dema as 'org.apache.iotdb.library.smoothing.UDTFDEMA'");
      statement.execute("create function TEMA as 'org.apache.iotdb.library.smoothing.UDTFTEMA'");
      statement.execute("create function RSI as 'org.apache.iotdb.library.smoothing.UDTFRSI'");
      statement.execute("create function TRIX as 'org.apache.iotdb.library.smoothing.UDTFTRIX'");
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
  public void testEMA1() {
    String sqlStr = "select ema(d1.s1,'window'='12') from root.season";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      while (resultSet.next()) {
        double result1 = resultSet.getDouble(1);
        Assert.assertTrue(Math.abs(result1) < 5.0);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDEMA1() {
    String sqlStr = "select dema(d1.s1,'window'='12') from root.season";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      while (resultSet.next()) {
        double result1 = resultSet.getDouble(1);
        Assert.assertTrue(Math.abs(result1) < 3.6);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTEMA1() {
    String sqlStr = "select tema(d1.s1,'window'='12') from root.season";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      while (resultSet.next()) {
        double result1 = resultSet.getDouble(1);
        Assert.assertTrue(Math.abs(result1) < 3.1);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testTRIX1() {
    String sqlStr = "select trix(d1.s1,'window'='12') from root.season";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      while (resultSet.next()) {
        double result1 = resultSet.getDouble(1);
        Assert.assertTrue(Math.abs(result1) < 5);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRSI1() {
    String sqlStr = "select rsi(d1.s1,'window'='12') from root.season";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      while (resultSet.next()) {
        double result1 = resultSet.getDouble(1);
        Assert.assertTrue(Math.abs(result1) < 5);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
