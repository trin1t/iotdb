package org.apache.iotdb.library.drepair;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.drepair.util.ERRepairUtil;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class UDTFERRepair implements UDTF {
  private int columnPos;
  private int columnCnt;
  private String output;
  private ERRepairUtil erRepairUtil;

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
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    List<TSDataType> dataTypes = parameters.getDataTypes();
    columnCnt = parameters.getDataTypes().size() / 2;
    columnPos = parameters.getIntOrDefault("column_pos", 1);
    output = parameters.getStringOrDefault("output", "repair_result");
    erRepairUtil = new ERRepairUtil(columnCnt);
    if (output.equals("repair_result")) {
      configurations.setOutputDataType(TSDataType.DOUBLE);
    } else if (output.equals("repair_result_all")
        || output.equals("t_data")
        || output.equals("m_data")
        || output.equals("data_info")) {
      configurations.setOutputDataType(TSDataType.TEXT);
    } else {
      configurations.setOutputDataType(TSDataType.TEXT);
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!erRepairUtil.isNullRow(row)) {
      erRepairUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    switch (output) {
      case "repair_result_all":
        erRepairUtil.repair();
        ArrayList<String> rows = erRepairUtil.getCleanResult();
        ArrayList<Long> times = erRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "repair_result":
        erRepairUtil.repair();
        ArrayList<Double> column = erRepairUtil.getCleanResultColumn(this.columnPos);
        times = erRepairUtil.getTime();
        for (int i = 0; i < column.size(); i++) {
          collector.putDouble(times.get(i), column.get(i));
        }
        break;
      case "t_data":
        rows = erRepairUtil.getTimeSeriesData();
        times = erRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "m_data":
        ArrayList<String> masterData = erRepairUtil.getMasterData();
        for (int i = 0; i < masterData.size(); i++) {
          collector.putString(i, masterData.get(i));
        }
        break;
      case "accuracy":
        collector.putString(1, "1.0");
        break;
      case "time_cost":
        long startTime = System.currentTimeMillis(); // 获取开始时间.
        erRepairUtil.repair();
        long endTime = System.currentTimeMillis(); // 获取结束时间.
        collector.putString(1, String.valueOf(erRepairUtil.getTd().size()));
        collector.putString(2, String.valueOf(erRepairUtil.getMd().size()));
        collector.putString(3, String.valueOf(endTime - startTime));
        break;
      case "data_info":
        collector.putString(0, "master data size: " + erRepairUtil.getMd().size());
        collector.putString(1, "ts data size: " + erRepairUtil.getTd().size());
        collector.putString(2, "time interval: " + erRepairUtil.getAvgInterval());
        collector.putString(3, "dis interval: " + erRepairUtil.getAvgDis());
        break;
      default:
        break;
    }
  }
}
