package org.apache.iotdb.library.drepair;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.drepair.util.KnnRepairUtil;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class UDTFKNNRepair implements UDTF {
  private int columnCnt;
  private int columnPos;
  private String output;
  private KnnRepairUtil knnRepairUtil;

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

    knnRepairUtil = new KnnRepairUtil(columnCnt);

    output = parameters.getStringOrDefault("output", "repair_result");
    if (output.equals("repair_result")) {
      configurations.setOutputDataType(TSDataType.DOUBLE);
    } else if (output.equals("repair_result_all")
        || output.equals("repair_log")
        || output.equals("t_data")
        || output.equals("m_data")
        || output.equals("test_kd_tree")
        || output.equals("data_info")
        || output.equals("fill_null")
        || output.equals("time_cost")) {
      configurations.setOutputDataType(TSDataType.TEXT);
    }
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!knnRepairUtil.isNullRow(row)) {
      knnRepairUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    switch (output) {
      case "repair_result_all":
        //        long startTime = System.currentTimeMillis(); // 获取开始时间.
        knnRepairUtil.buildKDTree();
        knnRepairUtil.repair();
        break;
      case "repair_result":
        knnRepairUtil.buildKDTree();
        knnRepairUtil.repair();
        ArrayList<Double> repair_result = knnRepairUtil.getTdCleanedColumn(columnPos);
        ArrayList<Long> times = knnRepairUtil.getTime();
        for (int i = 0; i < repair_result.size(); i++) {
          collector.putDouble(times.get(i), repair_result.get(i));
        }
        break;
      case "t_data":
        ArrayList<String> t_tuples = knnRepairUtil.getTimeSeriesData();
        ArrayList<Long> t_times = knnRepairUtil.getTime();
        for (int i = 0; i < t_tuples.size(); i++) {
          collector.putString(t_times.get(i), t_tuples.get(i));
        }
        break;
      case "m_data":
        ArrayList<String> masterData = knnRepairUtil.getMasterData();
        for (int i = 0; i < masterData.size(); i++) {
          collector.putString(i, masterData.get(i));
        }
        break;
      case "fill_null":
        knnRepairUtil.fillNullValue();
        t_tuples = knnRepairUtil.getTimeSeriesData();
        t_times = knnRepairUtil.getTime();
        for (int i = 0; i < t_tuples.size(); i++) {
          collector.putString(t_times.get(i), t_tuples.get(i));
        }
        break;
      case "time_cost":
        long startTime = System.currentTimeMillis(); // 获取开始时间.
        knnRepairUtil.buildKDTree();
        knnRepairUtil.repair();
        long endTime = System.currentTimeMillis(); // 获取结束时间.
        collector.putString(1, String.valueOf(endTime - startTime));
        break;
      default:
        break;
    }
  }
}
