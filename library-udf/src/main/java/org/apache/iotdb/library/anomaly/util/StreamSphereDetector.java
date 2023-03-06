/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.library.anomaly.util;

import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.rank.Median;

/**
 * This class implements UDTFSphereDetection
 */
public class StreamSphereDetector {
  public SphereTreeNode tree = new SphereTreeNode();
  private double madCoef = 3d;
  private ArrayList<Pair<Long, ArrayList<Double>>> PossibleOutliers = new ArrayList<>();
  private double distThreshold;
  private int anomalyThreshold;

  public StreamSphereDetector(double d, int l){
    distThreshold = d;
    anomalyThreshold = l;
  }

  public void initializeTree(ArrayList<Pair<Long, ArrayList<Double>>> points, int k){
    // k-means clustering
    int[] seeds = unrepeatedRandInt(k);
    ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
    ArrayList<Integer> clusters = new ArrayList<>();  // which cluster each point belongs to
    for(int i = 0; i < points.size(); i ++){
      clusters.add(-1);
    }
    for(int i = 0; i < k; i ++){
      clusters.set(seeds[i], i);
      centroids.add(new ArrayList<>(points.get(i).getRight()));
    }
    int MaxIter = 1000;
    for(int iter = 1; iter <= MaxIter; iter ++){
      boolean isStable = true;
      for(int i = 0; i < points.size(); i ++){
        double minDist = Double.MAX_VALUE;
        int cluster = -1;
        for(int j = 0; j < k; j ++){
          double dist = euDist(points.get(i).getRight(), centroids.get(j));
          if(dist < minDist){
            minDist = dist;
            cluster = j;
          }
        }
        if(cluster != clusters.get(i)){
          clusters.set(i, cluster);
          isStable = false;
        }
      }
      if(isStable){
        break;
      }
      else{
        // update centroids
        for (int i = 0; i < k; i ++){
          ArrayList<Double> center = new ArrayList<>();
          for(int j = 0; j < points.get(0).getRight().size(); j ++){
            center.add(0d);
          }
          int p = 0;
          for(int j = 0; j < points.size(); j ++){
            if(clusters.get(j) == i){
              for(int n = 0; n < points.get(0).getRight().size(); n ++){
                center.set(n, (center.get(n) * p + points.get(j).getRight().get(k))/ (p+1));
                p ++;
              }
            }
          }
          centroids.set(i, new ArrayList<>(center));
        }
      }
    }
    // find possible outliers
    ArrayList<ArrayList<Double>> dists = new ArrayList<>();
    double[] madDists = new double[k];
    for(int i = 0; i < k; i ++){
      dists.add(new ArrayList<>());
    }
    for(int i = 0; i < points.size(); i ++){
      dists.get(clusters.get(i)).add(euDist(points.get(i).getRight(),
          centroids.get(clusters.get(i))));
    }
    for(int i = 0; i < k; i ++){
      double[] ad = dists.get(i).stream().mapToDouble(Double::valueOf).toArray();
      double med = new Median().evaluate(ad);
      for(int j = 0; j < dists.get(i).size(); j ++){
        ad[i] = Math.abs(ad[i] - med);
      }
      double mad = new Median().evaluate(ad);
      for(int j = 0; j < dists.get(i).size(); j ++){
        if(Math.abs(dists.get(i).get(j) - med) > madCoef * mad){
          PossibleOutliers.add(points.get(j));
          clusters.set(j, -1);
        }
      }
    }
    // create spheres
    for(int i = 0; i < k; i ++){
      double r = 0;
      int pnum = 0;
      for (int j = 0; j < points.size(); j ++){
        if(clusters.get(j) == i){
          pnum ++;
          r = Math.max(r, dists.get(i).get(j));
        }
      }
      tree.sons.add(new SphereTreeNode(new Sphere(centroids.get(i), r, pnum)));
    }
  }

  public ArrayList<Long> flush(ArrayList<Pair<Long, ArrayList<Double>>> points){
    ArrayList<Long> outlierTimestamps = new ArrayList<>();
    long firstTimestamp = points.get(0).getLeft();
    ArrayList<Double> dists = new ArrayList<>();
    for(int i = 0; i < points.size(); i ++){
      SphereTreeNode n = tree;
      while(n.sons.size() > 0){
        boolean insideASonSphere = false;
        double min_dist_delta = Double.MAX_VALUE;
        Sphere nearestSphere = null;
        for(SphereTreeNode node : n.sons){
          double dist = euDist(node.sphere.getCenteriod(), points.get(i).getRight());
          if(dist <= node.sphere.getRadium()){
            node.sphere.setPointNum(node.sphere.getPointNum() + 1);
            insideASonSphere = true;
            break;
          }
          else if(min_dist_delta > dist - node.sphere.getRadium()){
            min_dist_delta = dist - n.sphere.getRadium();
            nearestSphere = n.sphere;
          }
        }
        if(! insideASonSphere){
          if(min_dist_delta < distThreshold && nearestSphere != null){
            nearestSphere.setPointNum(nearestSphere.getPointNum() + 1);
            nearestSphere.setRadium(nearestSphere.getRadium() + min_dist_delta);
          }
          break;
        }
      }
    }
    // try to construct new spheres
    SphereTreeNode newRoot = new SphereTreeNode();
    for(int i = 0; i < PossibleOutliers.size(); i ++){
      Sphere s = new Sphere(new ArrayList<>(PossibleOutliers.get(i).getRight()), distThreshold,0);
      s.addPoint(PossibleOutliers.get(i).getLeft());
      newRoot.sons.add(new SphereTreeNode(s));
    }
    PossibleOutliers.clear();
    mergeSpheres(newRoot, firstTimestamp);
    tree.sons.add(newRoot);
    // merge spheres
    outlierTimestamps.addAll(mergeSpheres(tree, firstTimestamp));
    return outlierTimestamps;
  }

  public ArrayList<Long> mergeSpheres(SphereTreeNode node, Long l){
    for(SphereTreeNode n : node.sons){
      if(n.sons.size() > 1){
        mergeSpheres(node, l);
      }
    }
    boolean isChanged;
    do{
      isChanged = false;
      for(SphereTreeNode s1 : node.sons){
        for(SphereTreeNode s2 : node.sons){
          if(s1 != s2){
            double centDist = euDist(s1.sphere.getCenteriod(), s2.sphere.getCenteriod());
            if(centDist * 1.5 < s1.sphere.getRadium() + s2.sphere.getRadium()){
              ArrayList<Double> newCentroid = newCenteriod(s1.sphere.getCenteriod(), s2.sphere.getCenteriod(), s1.sphere.getRadium(), s2.sphere.getRadium());
              SphereTreeNode newNode = new SphereTreeNode(new Sphere(newCentroid, centDist / 2, s1.sphere.getPointNum() + s2.sphere.getPointNum()));
              Long t1 = s1.sphere.points.removeFirst(), t2 = s2.sphere.points.removeFirst();
              if(newNode.sphere.getPointNum() > anomalyThreshold){
                newNode.sphere.setIsAnomaly(false);
              }
              else{
                while (s1.sphere.points.size() != 0 || s2.sphere.points.size() != 0) {
                  if (t1 < t2) {
                    newNode.sphere.points.addFirst(t1);
                    if (t1 < Long.MAX_VALUE && s1.sphere.points.size() > 0) {
                      t1 = s1.sphere.points.removeFirst();
                      newNode.sphere.points.addLast(t1);
                    } else {
                      t1 = Long.MAX_VALUE;
                    }
                  } else {
                    if (t2 < Long.MAX_VALUE && s2.sphere.points.size() > 0) {
                      t2 = s2.sphere.points.removeFirst();
                      newNode.sphere.points.addLast(t2);
                    } else {
                      t2 = Long.MAX_VALUE;
                    }
                  }
                }
              }
              node.sons.remove(s1);
              node.sons.remove(s2);
              node.sons.add(newNode);
              isChanged = true;
              break;
            }
          }
        }
        if(isChanged){
          break;
        }
      }
    }while (!isChanged);
    ArrayList<Long> res = new ArrayList<>();
    for(SphereTreeNode s : node.sons){
      if(s.sphere.isAnomaly()){
        while(s.sphere.points.size() > 0){
          Long t = s.sphere.points.getFirst();
          if(t < l){
            res.add(t);
            s.sphere.points.removeFirst();
          }
          else{
            break;
          }
        }
      }
    }
    return res;
  }

  private ArrayList<Double> newCenteriod(ArrayList<Double> o1, ArrayList<Double> o2, double r1, double r2){
    ArrayList<Double> res = new ArrayList<>();
    for(int i = 0; i < o1.size(); i ++){
      res.add(o1.get(i) * r2 / (r1 + r2) + o2.get(i) * r1 / (r1 + r2));
    }
    return res;
  }

  public double euDist(ArrayList<Double> x, ArrayList<Double> y){
    double dist = 0;
    if(x.size() == y.size()){
      for(int i = 0; i < x.size(); i ++){
        dist += (x.get(i) - y.get(i)) * (x.get(i) - y.get(i));
      }
      return Math.sqrt(dist);
    }
    return -1;
  }

  public int[] unrepeatedRandInt(int k) {
    int[] intRandom = new int[k];
    ArrayList<Integer> mylist = new ArrayList<>();
    Random rd = new Random();
    while(mylist.size() < k) {
      int num = rd.nextInt(k);
      if(!mylist.contains(num)) {
        mylist.add(num);
      }
    }
    for(int i = 0; i <mylist.size();i++) {
      intRandom[i] = mylist.get(i);
    }
    return intRandom;
  }
}
