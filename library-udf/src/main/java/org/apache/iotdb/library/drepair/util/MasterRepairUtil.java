package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class MasterRepairUtil {
  private final int NEIGHBORS_CNT = 5;
  private final int columnCnt;
  private final int[] precision = new int[] {2, 2, 2};
  private final int[] variance = new int[] {1, 1, 1000};
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_cleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private long omega;
  private int mu;
  private Double eta;
  private KDTree kdTree;
  private int[][] A;

  public MasterRepairUtil(int columnCnt, long omega, int mu, double eta) {
    this.columnCnt = columnCnt;
    this.omega = omega;
    this.mu = mu;
    this.eta = eta;
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

  public void buildKDTree() {
    this.kdTree = KDTree.build(md, this.columnCnt);
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
      double temp = t_tuple.get(pos) - m_tuple.get(pos);
      temp = temp / variance[pos];
      distance += temp * temp;
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public ArrayList<Integer> cal_P(int i) {
    ArrayList<Integer> p_i = new ArrayList<>();
    for (int l = 0; l < i; l++) {
      if (A[l][i] == 1) {
        p_i.add(l);
      }
    }
    return p_i;
  }

  public void cal_A() {
    A = new int[this.td.size()][];
    for (int i = 0; i < this.td.size(); i++) {
      A[i] = new int[this.td.size()]; // 动态创建第二维
      for (int j = 0; j < this.td.size(); j++) {
        A[i][j] = 0;
      }
    }

    for (int i = 0; i < this.td.size(); i++) {
      A[i][i] = 1;
      for (int j = i + 1; j < this.td.size(); j++) {
        if (j <= i + mu && this.td_time.get(j) <= this.td_time.get(i) + omega) {
          A[i][j] = 1;
          A[j][i] = 1;
        } else {
          break;
        }
      }
    }
  }

  public ArrayList<Double> cal_NearestP(ArrayList<Integer> P_i, int i) {
    double distance = Double.MAX_VALUE;
    ArrayList<Double> ori_tuple = this.td.get(i);
    int pos = -1;
    for (int j = 0; j < P_i.size(); j++) {
      double temp_dis = this.get_tm_distance(this.td.get(P_i.get(j)), ori_tuple);
      if (temp_dis < distance) {
        distance = temp_dis;
        pos = j;
      }
    }
    return this.td_cleaned.get(pos);
  }

  public double max_Delta(ArrayList<Double> c_j, ArrayList<Integer> P_i) {
    double max_dis = Double.MIN_VALUE;
    for (Integer integer : P_i) {
      double temp_dis = get_tm_distance(c_j, this.td_cleaned.get(integer));
      if (temp_dis > max_dis) {
        max_dis = temp_dis;
      }
    }
    return max_dis;
  }

  public ArrayList<String> testP() {
    ArrayList<String> Ps = new ArrayList<>();
    this.buildKDTree();
    cal_A();
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Integer> P_i = cal_P(i);
      Ps.add(P_i.toString());
    }
    return Ps;
  }

  public void master_repair() {
    this.fillNullValue();
    this.buildKDTree();
    cal_A();
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Integer> P_i = cal_P(i);
      if (P_i.size() == 0) {
        this.td_cleaned.add(this.kdTree.query(this.td.get(i)));
      } else {
        ArrayList<ArrayList<Double>> C_i = this.kdTree.queryKNN(this.td.get(i), NEIGHBORS_CNT);
        C_i.add(cal_NearestP(P_i, i));
        boolean added = false;
        for (ArrayList<Double> c_i : C_i) {
          double max_delta = max_Delta(c_i, P_i);
          if (max_delta <= eta) {
            added = true;
            this.td_cleaned.add(c_i);
            break;
          }
        }
        if (!added) this.td_cleaned.add(this.td.get(i));
      }
    }
  }

  public void repair() throws Exception {
    master_repair();
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

  public String getVariance() {
    double[] sums = new double[columnCnt];
    for (int i = 0; i < columnCnt; i++) {
      sums[i] = 0d;
    }
    for (ArrayList<Double> arrayList : this.td) {
      for (int j = 0; j < columnCnt; j++) {
        sums[j] += arrayList.get(j);
      }
    }
    double[] avgs = new double[columnCnt];
    for (int i = 0; i < columnCnt; i++) {
      avgs[i] = sums[i] / this.td.size();
    }

    double[] vars = new double[columnCnt];
    for (int i = 0; i < columnCnt; i++) {
      vars[i] = 0d;
    }
    for (ArrayList<Double> arrayList : this.td) {
      for (int j = 0; j < columnCnt; j++) {
        double temp = arrayList.get(j) - avgs[j];
        vars[j] += temp * temp;
      }
    }
    return Arrays.toString(vars);
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
