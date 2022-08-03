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

public class UDTFMasterRepair implements UDTF {
  private MasterRepairUtil masterRepairUtil;
  private int columnPos;
  private int columnCnt;
  private String output;
  private long omega;
  private Double eta;
  private int k;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator.validateInputSeriesDataType(
        0, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT32, TSDataType.INT64);
        if (validator.getParameters().hasAttribute("output_column")) {
          validator.validate(
              columnPos -> (int) columnPos > 0,
              "Parameter output_column should be larger than 1.",
              validator.getParameters().getInt("output_column"));
        }
        if (validator.getParameters().hasAttribute("omega")) {
          validator.validate(
              omega -> (int) omega > 0,
              "Parameter omega should be larger than 0.",
              validator.getParameters().getInt("omega"));
        }
        if (validator.getParameters().hasAttribute("eta")) {
          validator.validate(
              eta -> (double) eta > 0,
              "Parameter eta should be larger than 0.",
              validator.getParameters().getInt("mu"));
        }
        if (validator.getParameters().hasAttribute("k")) {
          validator.validate(
              eta -> (int) k > 0,
              "Parameter k should be larger than 0.",
              validator.getParameters().getInt("k"));
        }
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    columnCnt = parameters.getDataTypes().size() / 2;
    columnPos = parameters.getIntOrDefault("output_column", 1);
    output = parameters.getStringOrDefault("output", "repair_result");
    omega = parameters.getLongOrDefault("omega", -1);
    eta = parameters.getDoubleOrDefault("eta", Double.NaN);
    k = parameters.getIntOrDefault("k", -1);
    masterRepairUtil = new MasterRepairUtil(columnCnt, omega, eta, k);

    if (output.equals("repair_result")) {
      configurations.setOutputDataType(TSDataType.DOUBLE);
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
        //      case "testP":
        //        ArrayList<String> ps = masterRepairUtil.testP();
        //        times = masterRepairUtil.getTime();
        //        for (int i = 0; i < ps.size(); i++) {
        //          collector.putString(times.get(i), ps.get(i));
        //        }
        //        break;
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
        collector.putString(1, String.valueOf(endTime - startTime));
        break;
      case "data_info":
        //        masterRepairUtil.repair();
        collector.putString(0, "master data size: " + masterRepairUtil.getMd().size());
        collector.putString(1, "ts data size: " + masterRepairUtil.getTd().size());
        collector.putString(2, "time interval: " + masterRepairUtil.getAvgInterval());
        collector.putString(3, "dis interval: " + masterRepairUtil.getAvgDis());
        collector.putString(4, "variance: " + masterRepairUtil.getVariance());
        collector.putString(5, "cleaned data size: " + masterRepairUtil.getCleanResult().size());
        //        collector.putString(
        //            6, "cleaned data column 1 size: " +
        // masterRepairUtil.getCleanResultColumn(1).size());
        //        collector.putString(7, "time size: " + masterRepairUtil.getTime().size());
        break;
      default:
        break;
    }
  }
}
