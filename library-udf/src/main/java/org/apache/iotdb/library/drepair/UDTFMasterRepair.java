package org.apache.iotdb.library.drepair;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.drepair.util.MasterRepairUtil;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class UDTFMasterRepair implements UDTF {
  private MasterRepairUtil masterRepairUtil;
  private int columnPos;
  private int columnCnt;
  private String output;
  private long omega;
  private int mu;
  private Double eta;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesDataType(
        0, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT32, TSDataType.INT64);
    if (validator.getParameters().hasAttribute("column_pos")) {
      validator.validate(
          columnPos -> (int) columnPos > 0,
          "Parameter column_position should be larger than 1.",
          validator.getParameters().getInt("column_pos"));
    }
    if (validator.getParameters().hasAttribute("omega")) {
      validator.validate(
          omega -> (int) omega > 0,
          "Parameter omega should be larger than 0.",
          validator.getParameters().getInt("omega"));
    }
    if (validator.getParameters().hasAttribute("mu")) {
      validator.validate(
          mu -> (int) mu > 0,
          "Parameter mu should be larger than 0.",
          validator.getParameters().getInt("mu"));
    }
    if (validator.getParameters().hasAttribute("eta")) {
      validator.validate(
          eta -> (double) eta > 0,
          "Parameter mu should be larger than 0.",
          validator.getParameters().getInt("mu"));
    }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    List<TSDataType> dataTypes = parameters.getDataTypes();
    columnCnt = parameters.getDataTypes().size() / 2;
    columnPos = parameters.getIntOrDefault("column_position", 1);
    output = parameters.getStringOrDefault("output", "repair_result");
    omega = parameters.getLongOrDefault("omega", 150000);
    mu = parameters.getIntOrDefault("mu", 5);
    eta = parameters.getDoubleOrDefault("eta", 0.1);
    masterRepairUtil = new MasterRepairUtil(columnCnt, omega, mu, eta);
    if (output.equals("repair_result")) {
      configurations.setOutputDataType(dataTypes.get(columnPos - 1));
    } else if (output.equals("repair_result_all")
        || output.equals("repair_log")
        || output.equals("t_data")
        || output.equals("m_data")
        || output.equals("test_kd_tree")
        || output.equals("data_info")) {
      configurations.setOutputDataType(TSDataType.TEXT);
    } else {
      configurations.setOutputDataType(TSDataType.TEXT);
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!masterRepairUtil.isNullRow(row)) {
      masterRepairUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    switch (output) {
      case "repair_result_all":
        masterRepairUtil.repair();
        ArrayList<String> rows = masterRepairUtil.getCleanResult();
        ArrayList<Long> times = masterRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "repair_result":
        masterRepairUtil.repair();
        ArrayList<Double> column = masterRepairUtil.getCleanResultColumn(this.columnPos);
        times = masterRepairUtil.getTime();
        for (int i = 0; i < column.size(); i++) {
          collector.putDouble(times.get(i), column.get(i));
        }
        break;
      case "testP":
        ArrayList<String> ps = masterRepairUtil.testP();
        times = masterRepairUtil.getTime();
        for (int i = 0; i < ps.size(); i++) {
          collector.putString(times.get(i), ps.get(i));
        }
        break;
      case "t_data":
        rows = masterRepairUtil.getTimeSeriesData();
        times = masterRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "m_data":
        ArrayList<String> masterData = masterRepairUtil.getMasterData();
        for (int i = 0; i < masterData.size(); i++) {
          collector.putString(i, masterData.get(i));
        }
        break;
      case "accuracy":
        collector.putString(1, "1.0");
        break;
      case "time_cost":
        long startTime = System.currentTimeMillis(); // 获取开始时间.
        masterRepairUtil.repair();
        long endTime = System.currentTimeMillis(); // 获取结束时间.
        collector.putString(1, String.valueOf(masterRepairUtil.getTd().size()));
        collector.putString(2, String.valueOf(masterRepairUtil.getMd().size()));
        collector.putString(3, String.valueOf(endTime - startTime));
        break;
      case "data_info":
        masterRepairUtil.repair();
        collector.putString(0, "master data size: " + masterRepairUtil.getMd().size());
        collector.putString(1, "ts data size: " + masterRepairUtil.getTd().size());
        collector.putString(2, "time interval: " + masterRepairUtil.getAvgInterval());
        collector.putString(3, "dis interval: " + masterRepairUtil.getAvgDis());
        collector.putString(4, "variance: " + masterRepairUtil.getVariance());
        collector.putString(5, "cleaned data size: " + masterRepairUtil.getCleanResult().size());
        collector.putString(
            6, "cleaned data column 1 size: " + masterRepairUtil.getCleanResultColumn(1).size());
        collector.putString(7, "time size: " + masterRepairUtil.getTime().size());
        break;
      default:
        break;
    }
  }
}
