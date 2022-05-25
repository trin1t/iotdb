package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class ERRepairUtil {
  private final int columnCnt;
  private final double[] TOLERANCE = new double[] {0.0005, 0.0005, 10};
  private final int[] precision = new int[] {6, 6, 1};
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_cleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();

  public ERRepairUtil(int columnCnt) {
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

  public ArrayList<String> getCleanResult() {
    ArrayList<String> strings = new ArrayList<>();
    for (ArrayList<Double> tuple : this.td_cleaned) {
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

  public ArrayList<Double> getTimeSeriesColumn(int columnPos) {
    ArrayList<Double> column = new ArrayList<>();
    for (ArrayList<Double> tuple : td) {
      column.add(tuple.get(columnPos - 1));
    }
    return column;
  }

  public ArrayList<Double> getCleanResultColumn(int columnPos) {
    ArrayList<Double> column = new ArrayList<>();
    for (ArrayList<Double> tuple : this.td_cleaned) {
      column.add(tuple.get(columnPos - 1));
    }
    return column;
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

  public String toStr(ArrayList<Double> tuple) {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < tuple.size(); i++) {
      if (i != 0) {
        s.append(", ");
      }
      if (tuple.get(i) != null) {
        s.append(tuple.get(i).toString());
      }
    }
    return s.toString();
  }

  public ArrayList<Long> getTime() {
    return td_time;
  }

  public double get_tm_distance(ArrayList<Double> t_tuple, ArrayList<Double> m_tuple) {
    if (t_tuple.size() != m_tuple.size()) {
      return Double.MAX_VALUE;
    }

    ArrayList<Integer> NotNullPosition = new ArrayList<>();
    for (int i = 0; i < t_tuple.size(); i++) {
      if (t_tuple.get(i) != null) {
        NotNullPosition.add(i);
      }
    }

    double distance = new Double("0.0");

    for (Integer pos : NotNullPosition) {
      distance += (t_tuple.get(pos) - m_tuple.get(pos)) * (t_tuple.get(pos) - m_tuple.get(pos));
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public boolean checkIfComplete(ArrayList<Double> t_tuple) {
    for (Double value : t_tuple) {
      if (value == null) {
        return false;
      }
    }
    return true;
  }

  public boolean checkIfConsistent(ArrayList<Double> t_tuple) {
    for (ArrayList<Double> m_tuple : this.md) {
      boolean isConsistent = true;
      for (int k = 0; k < t_tuple.size(); k++) {
        if (Math.abs(t_tuple.get(k) - m_tuple.get(k)) > this.TOLERANCE[k]) {
          isConsistent = false;
          break;
        }
      }
      if (isConsistent) {
        return true;
      }
    }
    return false;
  }

  public boolean checkIfDirty(ArrayList<Double> t_tuple) {
    return !checkIfComplete(t_tuple) || !checkIfConsistent(t_tuple);
  }

  public ArrayList<Double> repairWithEditRule(
      ArrayList<Double> t_tuple, ArrayList<Double> m_tuple) {
    int null_cnt = 0;
    for (Double v : t_tuple) {
      if (v == null) {
        null_cnt++;
      }
    }

    if (null_cnt == 1) {
      if (t_tuple.get(0) == null) {
        if ((Math.abs(t_tuple.get(1) - m_tuple.get(1)) < this.TOLERANCE[1])
            && (Math.abs(t_tuple.get(2) - m_tuple.get(2)) < this.TOLERANCE[2])) {
          return m_tuple;
        }
      }

      if (t_tuple.get(1) == null) {
        if ((Math.abs(t_tuple.get(0) - m_tuple.get(0)) < this.TOLERANCE[0])
            && (Math.abs(t_tuple.get(2) - m_tuple.get(2)) < this.TOLERANCE[2])) {
          return m_tuple;
        }
      }

      if (t_tuple.get(2) == null) {
        if ((Math.abs(t_tuple.get(0) - m_tuple.get(0)) < this.TOLERANCE[0])
            && (Math.abs(t_tuple.get(1) - m_tuple.get(1)) < this.TOLERANCE[1])) {
          return m_tuple;
        }
      }
    } else if (null_cnt == 2) {
      if (t_tuple.get(0) != null && Math.abs(t_tuple.get(0) - m_tuple.get(0)) < this.TOLERANCE[0]) {
        return m_tuple;
      }

      if (t_tuple.get(1) != null && Math.abs(t_tuple.get(1) - m_tuple.get(1)) < this.TOLERANCE[1]) {
        return m_tuple;
      }

      if (t_tuple.get(2) != null && Math.abs(t_tuple.get(2) - m_tuple.get(2)) < this.TOLERANCE[2]) {
        return m_tuple;
      }
    } else {
      if (Math.abs(t_tuple.get(0) - m_tuple.get(0)) < this.TOLERANCE[0]
          && Math.abs(t_tuple.get(1) - m_tuple.get(1)) < this.TOLERANCE[1]
          && Math.abs(t_tuple.get(2) - m_tuple.get(2)) < this.TOLERANCE[2]) {
        return m_tuple;
      }
    }
    return new ArrayList<>();
  }

  public void er_repair() {
    ArrayList<Double> last_tuple = new ArrayList<>();
    for (int i = 0; i < columnCnt; i++) {
      last_tuple.add(0d);
    }
    for (ArrayList<Double> t_tuple : this.td) {
      if (checkIfDirty(t_tuple)) {
        boolean repaired = false;
        for (ArrayList<Double> m_tuple : this.md) {
          ArrayList<Double> result = this.repairWithEditRule(t_tuple, m_tuple);
          if (result.size() != 0) {
            this.td_cleaned.add(result);
            last_tuple = result;
            repaired = true;
            break;
          }
        }
        if (!repaired) {
          this.td_cleaned.add(last_tuple);
        }
      } else {
        this.td_cleaned.add(t_tuple);
        last_tuple = t_tuple;
      }
    }
  }

  public void repair() throws Exception {
    er_repair();
  }

  public ArrayList<ArrayList<Double>> getTd() {
    return td;
  }

  public ArrayList<ArrayList<Double>> getMd() {
    return md;
  }

  public long getAvgInterval() {
    long avg = 0;
    for (int i = 1; i < this.td_time.size(); i++) {
      long interval = this.td_time.get(i) - this.td_time.get(i - 1);
      avg += interval;
    }
    return avg / this.td_time.size();
  }

  public double getAvgDis() {
    fillNullValue();
    double dis = 0.0;
    for (int i = 1; i < this.td.size(); i++) {
      double temp_dis = this.get_tm_distance(this.td.get(i), this.td.get(i - 1));
      dis += temp_dis;
    }
    return dis / this.td.size();
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

  public double[] arrayListToList(ArrayList<Double> arrayList) {
    double[] doubles = new double[arrayList.size()];
    for (int i = 0; i < arrayList.size(); i++) {
      doubles[i] = arrayList.get(i);
    }
    return doubles;
  }

  public ArrayList<Double> listToArrayList(double[] list) {
    ArrayList<Double> arrayList = new ArrayList<>(list.length);
    for (int i = 0; i < list.length; i++) {
      arrayList.set(i, list[i]);
    }
    return arrayList;
  }
}
