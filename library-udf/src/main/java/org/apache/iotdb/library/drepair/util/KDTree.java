package org.apache.iotdb.library.drepair.util;

import java.util.*;

import static java.lang.Math.sqrt;

public class KDTree {
  private Node kdtree;

  private static class Node {
    // 分割的维度
    int partitiondimension;
    // 分割的值
    double partitionValue;
    // 如果为非叶子节点，该属性为空
    // 否则为数据
    ArrayList<Double> value;
    // 是否为叶子
    boolean isLeaf = false;
    // 左树
    Node left;
    // 右树
    Node right;
    // 每个维度的最小值
    ArrayList<Double> min;
    // 每个维度的最大值
    ArrayList<Double> max;
  }

  private static class UtilZ {

    static double variance(ArrayList<ArrayList<Double>> data, int dimension) {
      double sum = 0d;
      for (ArrayList<Double> d : data) {
        sum += d.get(dimension);
      }
      double avg = sum / data.size();
      double ans = 0d;
      for (ArrayList<Double> d : data) {
        double temp = d.get(dimension) - avg;
        ans += temp * temp;
      }
      return ans / data.size();
    }

    static double median(ArrayList<ArrayList<Double>> data, int dimension) {
      ArrayList<Double> d = new ArrayList<>();
      for (ArrayList<Double> k : data) {
        d.add(k.get(dimension));
      }
      Collections.sort(d);
      int pos = d.size() / 2;
      return d.get(pos);
    }

    static ArrayList<ArrayList<Double>> maxmin(ArrayList<ArrayList<Double>> data, int dimensions) {
      ArrayList<ArrayList<Double>> mm = new ArrayList<>();
      ArrayList<Double> min_v = new ArrayList<>();
      ArrayList<Double> max_v = new ArrayList<>();
      // 初始化 第一行为min，第二行为max
      for (int i = 0; i < dimensions; i++) {
        double min_temp = Double.MAX_VALUE;
        double max_temp = Double.MIN_VALUE;
        for (int j = 1; j < data.size(); j++) {
          ArrayList<Double> d = data.get(j);
          if (d.get(i) < min_temp) {
            min_temp = d.get(i);
          } else if (d.get(i) > max_temp) {
            max_temp = d.get(i);
          }
        }
        min_v.add(min_temp);
        max_v.add(max_temp);
      }
      mm.add(min_v);
      mm.add(max_v);
      return mm;
    }

    static double distance(ArrayList<Double> a, ArrayList<Double> b) {
      double sum = 0d;
      for (int i = 0; i < a.size(); i++) {
        if (a.get(i) != null && b.get(i) != null) sum += Math.pow(a.get(i) - b.get(i), 2);
      }
      sum = sqrt(sum);
      return sum;
    }

    static double mindistance(ArrayList<Double> a, ArrayList<Double> max, ArrayList<Double> min) {
      double sum = 0d;
      for (int i = 0; i < a.size(); i++) {
        if (a.get(i) > max.get(i)) sum += Math.pow(a.get(i) - max.get(i), 2);
        else if (a.get(i) < min.get(i)) {
          sum += Math.pow(min.get(i) - a.get(i), 2);
        }
      }
      sum = sqrt(sum);
      return sum;
    }
  }

  private void KDTree() {}

  public static KDTree build(ArrayList<ArrayList<Double>> input, int dimension) {
    KDTree tree = new KDTree();
    tree.kdtree = new Node();
    tree.buildDetail(tree.kdtree, input, dimension);
    return tree;
  }

  private void buildDetail(Node node, ArrayList<ArrayList<Double>> data, int dimensions) {
    if (data.size() == 0) {
      return;
    }
    if (data.size() == 1) {
      node.isLeaf = true;
      node.value = data.get(0);
      return;
    }
    // 选择方差最大的维度
    node.partitiondimension = -1;
    double var = -1;
    double tmpvar;
    for (int i = 0; i < dimensions; i++) {
      tmpvar = UtilZ.variance(data, i);
      if (tmpvar > var) {
        var = tmpvar;
        node.partitiondimension = i;
      }
    }
    // 如果方差=0，表示所有数据都相同，判定为叶子节点
    if (var == 0d) {
      node.isLeaf = true;
      node.value = data.get(0);
      return;
    }

    // 选择分割的值
    node.partitionValue = UtilZ.median(data, node.partitiondimension);

    ArrayList<ArrayList<Double>> maxmin = UtilZ.maxmin(data, dimensions);
    node.min = maxmin.get(0);
    node.max = maxmin.get(1);

    ArrayList<ArrayList<Double>> left = new ArrayList<>();
    ArrayList<ArrayList<Double>> right = new ArrayList<>();

    for (ArrayList<Double> d : data) {
      if (d.get(node.partitiondimension) < node.partitionValue) {
        left.add(d);
      } else if (d.get(node.partitiondimension) > node.partitionValue) {
        right.add(d);
      }
    }
    for (ArrayList<Double> d : data) {
      if (d.get(node.partitiondimension) == node.partitionValue) {
        if (left.size() == 0) {
          left.add(d);
        } else {
          right.add(d);
        }
      }
    }

    Node leftnode = new Node();
    Node rightnode = new Node();
    node.left = leftnode;
    node.right = rightnode;
    buildDetail(leftnode, left, dimensions);
    buildDetail(rightnode, right, dimensions);
  }

  public ArrayList<Double> query(ArrayList<Double> input) {
    Node node = kdtree;
    Stack<Node> stack = new Stack<>();
    while (!node.isLeaf) {
      if (input.get(node.partitiondimension) < node.partitionValue) {
        stack.add(node.right);
        node = node.left;
      } else {
        stack.push(node.left);
        node = node.right;
      }
    }

    double distance = UtilZ.distance(input, node.value);
    ArrayList<Double> nearest = queryRec(input, distance, stack);
    return nearest == null ? node.value : nearest;
  }

  public ArrayList<Double> queryRec(ArrayList<Double> input, double distance, Stack<Node> stack) {
    ArrayList<Double> nearest = null;
    Node node;
    double tdis;
    while (stack.size() != 0) {
      node = stack.pop();
      if (node.isLeaf) {
        tdis = UtilZ.distance(input, node.value);
        if (tdis < distance) {
          distance = tdis;
          nearest = node.value;
        }
      } else {
        /*
         * 得到该节点代表的超矩形中点到查找点的最小距离mindistance
         * 如果mindistance<distance表示有可能在这个节点的子节点上找到更近的点
         * 否则不可能找到
         */
        double mindistance = UtilZ.mindistance(input, node.max, node.min);
        if (mindistance < distance) {
          while (!node.isLeaf) {
            if (input.get(node.partitiondimension) < node.partitionValue) {
              stack.add(node.right);
              node = node.left;
            } else {
              stack.push(node.left);
              node = node.right;
            }
          }
          tdis = UtilZ.distance(input, node.value);
          if (tdis < distance) {
            distance = tdis;
            nearest = node.value;
          }
        }
      }
    }
    return nearest;
  }

  public ArrayList<ArrayList<Double>> queryKNN(ArrayList<Double> input, int k) {
    Node node = kdtree;
    Stack<Node> stack = new Stack<>();
    while (!node.isLeaf) {
      if (input.get(node.partitiondimension) < node.partitionValue) {
        stack.add(node.right);
        node = node.left;
      } else {
        stack.push(node.left);
        node = node.right;
      }
    }
    stack.push(node);
    return KNN(input, k, stack);
  }

  public ArrayList<ArrayList<Double>> KNN(ArrayList<Double> input, int k, Stack<Node> stack) {
    ArrayList<ArrayList<Double>> kNearest = new ArrayList<>();
    PriorityQueue<TupleWithDistance> priorityQueue = new PriorityQueue<>();

    Node node;
    double tdis;
    while (stack.size() != 0) {
      node = stack.pop();
      while (!node.isLeaf) {
        if (input.get(node.partitiondimension) < node.partitionValue) {
          stack.add(node.right);
          node = node.left;
        } else {
          stack.push(node.left);
          node = node.right;
        }
      }
      tdis = UtilZ.distance(input, node.value);
      priorityQueue.add(new TupleWithDistance(tdis, node.value));
      if (priorityQueue.size() > k * k) {
        break;
      }
    }
    for (int i = 0; i < k; i++) {
      assert priorityQueue.peek() != null;
      kNearest.add(priorityQueue.peek().getTuple());
      priorityQueue.poll();
    }
    return kNearest;
  }

  public static class TupleWithDistance implements Comparable<TupleWithDistance> {
    private final Double distance;
    private final ArrayList<Double> tuple;

    public TupleWithDistance(Double distance, ArrayList<Double> tuple) {
      this.distance = distance;
      this.tuple = tuple;
    }

    @Override
    public int compareTo(TupleWithDistance t) {
      return this.distance.compareTo(t.distance);
    }

    public Double getDistance() {
      return distance;
    }

    public ArrayList<Double> getTuple() {
      return tuple;
    }
  }
}
