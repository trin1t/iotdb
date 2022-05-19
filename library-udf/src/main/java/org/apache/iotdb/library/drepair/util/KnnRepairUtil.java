package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

public class KnnRepairUtil {
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_cleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private final int[] precision = new int[] {1, 1, 1};
  private int columnCnt;
  private KDTree kdTree;

  public KnnRepairUtil(int columnCnt) {
    this.columnCnt = columnCnt;
  }

  public boolean isNullRow(Row row) {
    boolean flag = true;
    for (int i = 0; i < row.size(); i++) {
      if (!row.isNull(i)) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  public void addRow(Row row) throws IOException, NoNumberException {
    ArrayList<Double> tt = new ArrayList<>(); // time-series tuple
    boolean containsNotNull = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        double test = bd.setScale(precision[i], BigDecimal.ROUND_DOWN).doubleValue();
        tt.add(test);
      } else {
        tt.add(null);
      }
    }
    if (containsNotNull) {
      td.add(tt);
      td_time.add(row.getTime());
    }

    ArrayList<Double> mt = new ArrayList<>(); // master tuple
    containsNotNull = false;
    for (int i = this.columnCnt; i < row.size(); i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        BigDecimal bd = BigDecimal.valueOf(Util.getValueAsDouble(row, i));
        double test =
            bd.setScale(precision[i - this.columnCnt], BigDecimal.ROUND_DOWN).doubleValue();
        mt.add(test);
      } else {
        mt.add(null);
      }
    }
    if (containsNotNull) {
      md.add(mt);
    }
  }

  public ArrayList<String> getTimeSeriesData() {
    ArrayList<String> strings = new ArrayList<>();
    for (ArrayList<Double> tuple : td) {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < tuple.size(); i++) {
        if (i != 0) {
          s.append(",");
        }
        if (tuple.get(i) != null) {
          s.append(tuple.get(i).toString());
        }
      }
      strings.add(s.toString());
    }
    return strings;
  }

  public ArrayList<String> getMasterData() {
    ArrayList<String> strings = new ArrayList<>();
    for (ArrayList<Double> doubles : md) {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < doubles.size(); i++) {
        if (i != 0) {
          s.append(", ");
        }
        s.append(doubles.get(i).toString());
      }
      strings.add(s.toString());
    }
    return strings;
  }

  public ArrayList<Long> getTime() {
    return td_time;
  }

  public ArrayList<ArrayList<Double>> getTdCleaned() {
    return td_cleaned;
  }

  public ArrayList<Double> getTdCleanedColumn(int columnPos) {
    ArrayList<Double> column = new ArrayList<>();
    for (ArrayList<Double> tuple : td_cleaned) {
      column.add(tuple.get(columnPos - 1));
    }
    return column;
  }

  public void buildKDTree() {
    this.kdTree = KDTree.build(md, this.columnCnt);
  }

  public void fillNullValue() {
    for (int i = 0; i < columnCnt; i++) {
      double temp = this.td.get(0).get(i);
      for (ArrayList<Double> arrayList : this.td) {
        if (arrayList.get(i) == null) {
          arrayList.set(i, temp);
          //          this.td.set(j)
        } else {
          temp = arrayList.get(i);
        }
      }
    }
  }

  public void repair() {
    this.fillNullValue();
    for (ArrayList<Double> tuple : this.td) {
      ArrayList<Double> rt = this.kdTree.query(tuple);
      td_cleaned.add(rt);
    }
  }
}
