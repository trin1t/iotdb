package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class LocalMasterRepairUtil {
  private final int THRESHOLD = 5;
  private final int NEIGHBORS_CNT = 5;
  private final ArrayList<BigDecimal> TOLERANCE = new ArrayList<>();
  private final ArrayList<BigDecimal> MaxValues = new ArrayList<>();
  private final BigDecimal TOLERANCE_RATE = new BigDecimal("0.05");
  private final BigDecimal ALPHA = new BigDecimal("0.9");
  private final BigDecimal EWMA_ALPHA = new BigDecimal("0.9");
  private final int columnCnt;
  private final int WINDOW_SIZE = 10;
  private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final ArrayList<ArrayList<BigDecimal>> td = new ArrayList<>();
  private final ArrayList<ArrayList<BigDecimal>> md = new ArrayList<>();
  private final ArrayList<ArrayList<BigDecimal>> dirty_tuple = new ArrayList<>();
  private final ArrayList<Long> td_time = new ArrayList<>();
  private final ArrayList<Long> dirty_tuple_time = new ArrayList<>();
  private List<TSDataType> dataTypes = new ArrayList<>();

  public LocalMasterRepairUtil(List<TSDataType> dataTypes, int columnCnt) {
    this.dataTypes = dataTypes;
    this.columnCnt = columnCnt;
  }

  public LocalMasterRepairUtil(int columnCnt) {
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

  public void updateMax(ArrayList<BigDecimal> m_tuple) {
    for (int i = 0; i < columnCnt; i++) {
      if (m_tuple.get(i).compareTo(this.MaxValues.get(i)) > 0) {
        this.MaxValues.set(i, m_tuple.get(i));
      }
    }
  }

  public void initMAXValues() {
    for (int i = 0; i < columnCnt; i++) {
      this.MaxValues.add(new BigDecimal("-100000"));
    }
  }

  public void addRow(Row row) throws IOException, NoNumberException {
    ArrayList<BigDecimal> tt = new ArrayList<>(); // time-series tuple
    boolean containsNotNull = false;
    for (int i = 0; i < this.columnCnt; i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        tt.add(Util.getValueAsBigDecimal(row, i));
      } else {
        tt.add(null);
      }
    }
    if (containsNotNull) {
      td.add(tt);
      td_time.add(row.getTime());
    }

    ArrayList<BigDecimal> mt = new ArrayList<>(); // master tuple
    containsNotNull = false;
    for (int i = this.columnCnt; i < row.size(); i++) {
      if (!row.isNull(i)) {
        containsNotNull = true;
        mt.add(Util.getValueAsBigDecimal(row, i));
      } else {
        mt.add(null);
      }
    }
    if (containsNotNull) {
      md.add(mt);
      this.updateMax(mt);
    }
  }

  public ArrayList<String> getTimeSeriesData() {
    ArrayList<String> strings = new ArrayList<>();
    for (ArrayList<BigDecimal> tuple : td) {
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
    for (ArrayList<BigDecimal> tuple : td) {
      column.add(tuple.get(columnPos - 1).doubleValue());
    }
    return column;
  }

  public ArrayList<String> getMasterData() {
    ArrayList<String> strings = new ArrayList<>();
    for (ArrayList<BigDecimal> doubles : md) {
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

  public String toStr(ArrayList<BigDecimal> tuple) {
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

  public BigDecimal get_tm_distance(
      ArrayList<BigDecimal> t_tuple, ArrayList<BigDecimal> m_tuple, String method)
      throws Exception {
    if (t_tuple.size() != m_tuple.size()) {
      throw new Exception("Error: Missing column.");
    }
    BigDecimal distance;

    ArrayList<Integer> NotNullPosition = new ArrayList<>();
    for (int i = 0; i < t_tuple.size(); i++) {
      if (t_tuple.get(i) != null) {
        NotNullPosition.add(i);
      }
    }
    MathContext mc = new MathContext(100, RoundingMode.HALF_UP);
    switch (method) {
      case "value_method":
        distance = new BigDecimal("0.0");
        for (Integer pos : NotNullPosition) {
          distance =
              distance.add(
                  (t_tuple.get(pos).subtract(m_tuple.get(pos)))
                      .multiply((t_tuple.get(pos).subtract(m_tuple.get(pos)))));
        }
        // todo:sqrt
        break;
      case "count_method":
        distance = new BigDecimal("0");
        ;
        for (Integer pos : NotNullPosition) {
          if (t_tuple
                  .get(pos)
                  .subtract(m_tuple.get(pos))
                  .abs()
                  .compareTo(new BigDecimal(this.THRESHOLD))
              > 0) {
            distance = distance.add(new BigDecimal(1));
          }
        }
        break;
      default:
        throw new Exception();
    }
    return distance;
  }

  public ArrayList<ArrayList<BigDecimal>> dequeToArrayList(Deque<ArrayList<BigDecimal>> deque) {
    ArrayList<ArrayList<BigDecimal>> arrayLists = new ArrayList<>();
    while (!deque.isEmpty()) {
      arrayLists.add(deque.poll());
    }
    return arrayLists;
  }

  public ArrayList<Long> dequeToArrayListLong(Deque<Long> deque) {
    ArrayList<Long> arrayLists = new ArrayList<>();
    while (!deque.isEmpty()) {
      arrayLists.add(deque.poll());
    }
    return arrayLists;
  }

  public BigDecimal get_cT_distance(
      ArrayList<BigDecimal> c_tuple, Deque<ArrayList<BigDecimal>> window, String method)
      throws Exception {
    BigDecimal distance = new BigDecimal("0.0");
    switch (method) {
      case "weight_method":
        ArrayList<ArrayList<BigDecimal>> T_window = dequeToArrayList(window);
        for (int i = 0; i < T_window.size(); i++) {
          ArrayList<BigDecimal> t_tuple = T_window.get(i);
          BigDecimal single_distance = get_tm_distance(t_tuple, c_tuple, "value_method");
          distance =
              distance.add(
                  single_distance.multiply(
                      BigDecimal.valueOf(Math.pow(ALPHA.doubleValue(), T_window.size() - i))));
        }
        break;
      case "forecast_method":
        break;
      default:
        throw new Exception();
    }
    return distance;
  }

  public boolean checkIfComplete(ArrayList<BigDecimal> t_tuple) {
    for (BigDecimal value : t_tuple) {
      if (value == null) {
        return false;
      }
    }
    return true;
  }

  public boolean checkIfConsistent(ArrayList<BigDecimal> t_tuple) {
    for (ArrayList<BigDecimal> m_tuple : this.md) {
      boolean isConsistent = true;
      for (int k = 0; k < t_tuple.size(); k++) {
        if (t_tuple.get(k).subtract(m_tuple.get(k)).abs().compareTo(this.TOLERANCE.get(k)) > 0) {
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

  public boolean checkIfDirty(ArrayList<BigDecimal> t_tuple) {
    return !checkIfComplete(t_tuple) || !checkIfConsistent(t_tuple);
  }

  public ArrayList<String> getDirtyTuplePos() {
    ArrayList<String> positions = new ArrayList<>();
    for (int i = 0; i < td.size(); i++) {
      ArrayList<BigDecimal> t_tuple = td.get(i);
      if (checkIfDirty(t_tuple)) {
        positions.add(String.valueOf(i));
      }
    }
    return positions;
  }

  public ArrayList<ArrayList<BigDecimal>> get_candidates(ArrayList<BigDecimal> t_tuple)
      throws Exception {
    PriorityQueue<TupleWithDistance> priorityQueue = new PriorityQueue<>();
    for (ArrayList<BigDecimal> m_tuple : this.md) {
      BigDecimal distance = get_tm_distance(t_tuple, m_tuple, "value_method");
      priorityQueue.add(new TupleWithDistance(distance, m_tuple));
    }
    ArrayList<ArrayList<BigDecimal>> candidates = new ArrayList<>();
    for (int i = 0; i < Math.min(NEIGHBORS_CNT, priorityQueue.size()); i++) {
      assert priorityQueue.peek() != null;
      candidates.add(priorityQueue.peek().getTuple());
      priorityQueue.poll();
    }
    return candidates;
  }

  public ArrayList<String> repairWithLogs() throws Exception {
    this.preProcess();
    ArrayList<String> repair_logs = new ArrayList<>();
    Deque<ArrayList<BigDecimal>> window = new LinkedList<>();

    for (int i = 0; i < td.size(); i++) {
      ArrayList<BigDecimal> t_tuple = td.get(i);
      if (checkIfDirty(t_tuple)) {
        this.dirty_tuple.add(t_tuple);
        this.dirty_tuple_time.add(td_time.get(i));
        ArrayList<ArrayList<BigDecimal>> candidates = this.get_candidates(t_tuple);
        PriorityQueue<TupleWithDistance> priorityQueue = new PriorityQueue<>();
        for (ArrayList<BigDecimal> candidate : candidates) {
          BigDecimal distance = get_cT_distance(candidate, window, "weight_method");
          priorityQueue.add(new TupleWithDistance(distance, candidate));
        }
        assert priorityQueue.peek() != null;
        ArrayList<BigDecimal> repair_tuple = priorityQueue.peek().getTuple();

        repair_logs.add(
            dateFormat.format(new Date(this.td_time.get(i)))
                + ": "
                + this.toStr(t_tuple)
                + " -> "
                + this.toStr(repair_tuple));
      }
      window.add(t_tuple);
      if (window.size() > WINDOW_SIZE) {
        window.poll();
      }
    }
    return repair_logs;
  }

  public void preProcess() {
    for (int i = 0; i < columnCnt; i++) {
      this.TOLERANCE.add(this.MaxValues.get(i).multiply(TOLERANCE_RATE));
    }
  }

  public void master_repair() throws Exception {
    Deque<ArrayList<BigDecimal>> window = new LinkedList<>();
    for (int i = 0; i < td.size(); i++) {
      ArrayList<BigDecimal> t_tuple = td.get(i);
      if (checkIfDirty(t_tuple)) {
        this.dirty_tuple.add(t_tuple);
        this.dirty_tuple_time.add(td_time.get(i));
        ArrayList<ArrayList<BigDecimal>> candidates = this.get_candidates(t_tuple);
        PriorityQueue<TupleWithDistance> priorityQueue = new PriorityQueue<>();
        for (ArrayList<BigDecimal> candidate : candidates) {
          BigDecimal distance = get_cT_distance(candidate, window, "weight_method");
          priorityQueue.add(new TupleWithDistance(distance, candidate));
        }
        assert priorityQueue.peek() != null;
        ArrayList<BigDecimal> repair_tuple = priorityQueue.peek().getTuple();
        td.set(i, repair_tuple);
      }
      window.add(t_tuple);
      if (window.size() > WINDOW_SIZE) {
        window.poll();
      }
    }
  }

  public void repair(String method) throws Exception {
    if (method.equals("master_repair")) {
      master_repair();
    }
  }

  public ArrayList<Long> getDirty_tuple_time() {
    return dirty_tuple_time;
  }

  public ArrayList<ArrayList<BigDecimal>> getDirty_tuple() {
    ArrayList<ArrayList<BigDecimal>> dirty_tuples = new ArrayList<>();
    for (ArrayList<BigDecimal> doubles : td) {
      if (checkIfDirty(doubles)) {
        dirty_tuples.add(doubles);
      }
    }
    return dirty_tuples;
  }

  public ArrayList<ArrayList<BigDecimal>> getTd() {
    return td;
  }

  public ArrayList<ArrayList<BigDecimal>> getMd() {
    return md;
  }

  public static class TupleWithDistance implements Comparable<TupleWithDistance> {
    private final BigDecimal distance;
    private final ArrayList<BigDecimal> tuple;

    public TupleWithDistance(BigDecimal distance, ArrayList<BigDecimal> tuple) {
      this.distance = distance;
      this.tuple = tuple;
    }

    @Override
    public int compareTo(TupleWithDistance t) {
      return this.distance.compareTo(t.distance);
    }

    public BigDecimal getDistance() {
      return distance;
    }

    public ArrayList<BigDecimal> getTuple() {
      return tuple;
    }
  }

  public ArrayList<BigDecimal> getMaxValues() {
    return MaxValues;
  }

  public ArrayList<BigDecimal> getTOLERANCE() {
    return TOLERANCE;
  }

  public ArrayList<ArrayList<BigDecimal>> getCleanTuples() {
    ArrayList<ArrayList<BigDecimal>> clean = new ArrayList<>();
    for (ArrayList<BigDecimal> doubles : td) {
      if (!checkIfDirty(doubles)) {
        clean.add(doubles);
      }
    }
    return clean;
  }
}
