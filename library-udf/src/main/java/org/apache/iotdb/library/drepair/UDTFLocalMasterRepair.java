package org.apache.iotdb.library.drepair;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.library.drepair.util.LocalMasterRepairUtil;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class UDTFLocalMasterRepair implements UDTF {
  private LocalMasterRepairUtil LocalmasterRepairUtil;
  private int columnPos;
  private String method;
  private String output;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesDataType(
            0, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT32, TSDataType.INT64)
        .validate(
            columnPos -> (int) columnPos > 0,
            "Parameter column_position should be larger than 1.",
            validator.getParameters().getIntOrDefault("column_position", 1))
        .validateInputSeriesNumber(2 * validator.getParameters().getInt("column_number"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy());
    List<TSDataType> dataTypes = parameters.getDataTypes();
    int columnCnt = parameters.getDataTypes().size();
    columnPos = parameters.getIntOrDefault("column_position", 1);
    LocalmasterRepairUtil = new LocalMasterRepairUtil(dataTypes, columnCnt);
    method = parameters.getStringOrDefault("method", "master_repair");
    output = parameters.getStringOrDefault("output", "repair_result");
    configurations.setOutputDataType(TSDataType.DOUBLE);
    LocalmasterRepairUtil.initMAXValues();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!LocalmasterRepairUtil.isNullRow(row)) {
      LocalmasterRepairUtil.addRow(row);
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    LocalmasterRepairUtil.preProcess();
    switch (output) {
      case "repair_result":
        LocalmasterRepairUtil.repair(method);
        ArrayList<String> rows = LocalmasterRepairUtil.getTimeSeriesData();
        ArrayList<Long> times = LocalmasterRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "repair_column":
        LocalmasterRepairUtil.repair(method);
        ArrayList<Double> column = LocalmasterRepairUtil.getTimeSeriesColumn(this.columnPos);
        times = LocalmasterRepairUtil.getTime();
        for (int i = 0; i < column.size(); i++) {
          collector.putDouble(times.get(i), column.get(i));
        }
        break;
      case "repair_log":
        ArrayList<String> repair_logs = LocalmasterRepairUtil.repairWithLogs();
        for (int i = 0; i < repair_logs.size(); i++) {
          collector.putString(i, repair_logs.get(i));
        }
        break;
      case "dirty_tuples":
        ArrayList<ArrayList<BigDecimal>> dirty_tuple = LocalmasterRepairUtil.getDirty_tuple();
        for (int i = 0; i < dirty_tuple.size(); i++) {
          collector.putString(i, LocalmasterRepairUtil.toStr(dirty_tuple.get(i)));
        }
        break;
      case "t_data":
        rows = LocalmasterRepairUtil.getTimeSeriesData();
        times = LocalmasterRepairUtil.getTime();
        for (int i = 0; i < rows.size(); i++) {
          collector.putString(times.get(i), rows.get(i));
        }
        break;
      case "m_data":
        ArrayList<String> masterData = LocalmasterRepairUtil.getMasterData();
        for (int i = 0; i < masterData.size(); i++) {
          collector.putString(i, masterData.get(i));
        }
        break;
      case "dirty_tuple_pos":
        ArrayList<String> positions = LocalmasterRepairUtil.getDirtyTuplePos();
        for (int i = 0; i < positions.size(); i++) {
          collector.putString(i, positions.get(i));
        }
        break;
      case "accuracy":
        collector.putString(1, "1.0");
        break;
      case "time_cost":
        long startTime = System.currentTimeMillis(); // 获取开始时间.
        LocalmasterRepairUtil.repair(method);
        long endTime = System.currentTimeMillis(); // 获取结束时间.
        collector.putString(1, String.valueOf(LocalmasterRepairUtil.getTd().size()));
        collector.putString(2, String.valueOf(LocalmasterRepairUtil.getMd().size()));
        collector.putString(3, String.valueOf(endTime - startTime));
        break;
      case "pre_info":
        ArrayList<BigDecimal> t = LocalmasterRepairUtil.getTOLERANCE();
        ArrayList<BigDecimal> m = LocalmasterRepairUtil.getMaxValues();
        collector.putString(1, t.get(0).toString());
        collector.putString(2, t.get(1).toString());
        collector.putString(3, t.get(2).toString());
        collector.putString(4, m.get(0).toString());
        collector.putString(5, m.get(1).toString());
        collector.putString(6, m.get(2).toString());
      case "clean_tuples":
        ArrayList<ArrayList<BigDecimal>> c_ts = LocalmasterRepairUtil.getCleanTuples();
        for (int i = 0; i < c_ts.size(); i++) {
          collector.putString(i, LocalmasterRepairUtil.toStr(c_ts.get(i)));
        }
        break;
      case "data_info":
        collector.putString(0, "master data size: " + LocalmasterRepairUtil.getMd().size());
        collector.putString(1, "ts data size: " + LocalmasterRepairUtil.getTd().size());
        collector.putString(
            2, "clean_tuples size: " + LocalmasterRepairUtil.getCleanTuples().size());
        collector.putString(
            3, "dirty_tuples size: " + LocalmasterRepairUtil.getDirty_tuple().size());
        break;
      case "time_intervals":
        times = LocalmasterRepairUtil.getTime();
        for (int i = 2; i < times.size(); i++) {
          collector.putLong(i - 1, times.get(i) - times.get(i - 1));
        }
        break;
      default:
        break;
    }
  }
}
