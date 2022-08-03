package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class MasterRepairUtil {
  //  private final int[] precision = {3, 3, 3};
  private final int columnCnt;
  private final ArrayList<ArrayList<Double>> td = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> td_cleaned = new ArrayList<>();
  private final ArrayList<ArrayList<Double>> md = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private long omega;
  private Double eta;
  private int k;
  private double[] std;
  private long interval;
  private KDTree kdTree;

  public MasterRepairUtil(int columnCnt, long omega, double eta, int k) throws Exception {
    this.columnCnt = columnCnt;
    this.omega = omega;
    this.eta = eta;
    this.k = k;
  }

  public void buildKDTree() {
    this.kdTree = KDTree.build(md, this.columnCnt);
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
        //        double test = bd.setScale(precision[i], BigDecimal.ROUND_DOWN).doubleValue();
        //        tt.add(test);
        tt.add(bd.doubleValue());
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
        //        double test =
        //            bd.setScale(precision[i - this.columnCnt],
        // BigDecimal.ROUND_DOWN).doubleValue();
        //        mt.add(test);
        mt.add(bd.doubleValue());
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
    double distance = 0d;
    for (int pos = 0; pos < columnCnt; pos++) {
      double temp = t_tuple.get(pos) - m_tuple.get(pos);
      temp = temp / std[pos];
      distance += temp * temp;
    }
    distance = Math.sqrt(distance);
    return distance;
  }

  public ArrayList<Integer> cal_T(int i) {
    ArrayList<Integer> T_i = new ArrayList<>();
    for (int l = i - 1; l >= 0; l--) {
      if (this.td_time.get(i) <= this.td_time.get(l) + omega) {
        T_i.add(l);
      }
    }
    return T_i;
  }

  public ArrayList<ArrayList<Double>> cal_C(int i, ArrayList<Integer> T_i) {
    ArrayList<ArrayList<Double>> C_i = new ArrayList<>();
    if (T_i.size() == 0) {
      C_i.add(this.kdTree.query(this.td.get(i), std));
    } else {
      //            C_i.add(this.kdTree.query(this.td.get(i), std));
      C_i.addAll(this.kdTree.queryKNN(this.td.get(i), k, std));
      for (Integer integer : T_i) {
        C_i.addAll(this.kdTree.queryKNN(this.td_cleaned.get(integer), k, std));
      }
    }
    return C_i;
  }

  public void master_repair() {
    for (int i = 0; i < this.td.size(); i++) {
      ArrayList<Double> tuple = this.td.get(i);
      ArrayList<Integer> T_i = cal_T(i);
      ArrayList<ArrayList<Double>> C_i = this.cal_C(i, T_i);
      double min_dis = Double.MAX_VALUE;
      ArrayList<Double> repair_tuple = new ArrayList<>();
      for (ArrayList<Double> c_i : C_i) {
        boolean smooth = true;
        for (Integer t_i : T_i) {
          ArrayList<Double> t_is = td_cleaned.get(t_i);
          if (get_tm_distance(c_i, t_is) > eta) {
            smooth = false;
            break;
          }
        }
        if (smooth) {
          double dis = get_tm_distance(c_i, tuple);
          if (dis < min_dis) {
            min_dis = dis;
            repair_tuple = c_i;
          }
        }
      }
      this.td_cleaned.add(repair_tuple);
    }
  }

  public void set_parameters() {
    //    TODO
  }

  private double varianceImperative(double[] value) {
    double average = 0.0;
    int cnt = 0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        cnt += 1;
        average += p;
      }
    }
    if (cnt == 0) {
      return 0d;
    }
    average /= cnt;

    double variance = 0.0;
    for (double p : value) {
      if (!Double.isNaN(p)) {
        variance += (p - average) * (p - average);
      }
    }
    return variance / cnt;
  }

  private double[] getColumn(int pos) {
    double[] column = new double[this.td.size()];
    for (int i = 0; i < this.td.size(); i++) {
      column[i] = this.td.get(i).get(pos);
    }
    return column;
  }

  public void call_std() {
    this.std = new double[this.columnCnt];
    for (int i = 0; i < this.columnCnt; i++) {
      std[i] = Math.sqrt(varianceImperative(getColumn(i)));
    }
  }

  public void repair() throws Exception {
    fillNullValue();
    buildKDTree();
    call_std();
    set_parameters();
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
