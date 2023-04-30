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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;

/** Util for SphereDetection */
public class SphereDetectionUtil {

  public static double dist(Sphere s1, Sphere s2) throws Exception {
    int dim = s1.getCenteriod().size();
    if (dim != s2.getCenteriod().size()) {
      throw new Exception("Different dimensions.");
    }
    ArrayList<Double> c1 = s1.getCenteriod();
    ArrayList<Double> c2 = s2.getCenteriod();
    double dist = 0;
    for (int i = 0; i < dim; i++) {
      dist += Math.pow((c1.get(i) - c2.get(i)), 2);
    }
    dist = Math.sqrt(dist);
    dist -= s1.getRadium();
    dist -= s2.getRadium();
    return dist;
  }

  public static double centerDist(Sphere s1, Sphere s2) throws Exception {
    return euDist(s1.getCenteriod(), s2.getCenteriod());
  }

  public static Sphere merge(Sphere s1, Sphere s2) throws Exception {
    ArrayList<Double> c1 = s1.getCenteriod();
    ArrayList<Double> c2 = s2.getCenteriod();
    int dim = s1.getCenteriod().size();
    double r1 = s1.getRadium();
    double r2 = s2.getRadium();
    double centd = centerDist(s1, s2);
    if (centd <= Math.abs(r1 - r2)) { // one sphere is inside another
      if (r1 < r2) {
        Sphere res = new Sphere(c2, r2, s1.getPointNum() + s2.getPointNum());
        res.setIsAnomaly(s2.isAnomaly());
        ArrayDeque<Long> points = s1.getPoints();
        for (Long p : points) {
          res.addPoint(p);
        }
        points = s2.getPoints();
        for (Long p : points) {
          res.addPoint(p);
        }
        res.sons.add(s1);
        res.sons.add(s2);
        return res;
      } else {
        Sphere res = new Sphere(c1, r1, s1.getPointNum() + s2.getPointNum());
        res.setIsAnomaly(s1.isAnomaly());
        ArrayDeque<Long> points = s1.getPoints();
        if(points != null){
          for (Long p : points) {
            res.addPoint(p);
          }
        }
        points = s2.getPoints();
        if(points != null){
          for (Long p : points) {
            res.addPoint(p);
          }
        }
        res.sons.add(s1);
        res.sons.add(s2);
        return res;
      }
    } else {
      ArrayList<Double> c3 = new ArrayList<>();
      for (int i = 0; i < dim; i++) {
        c3.add((c1.get(i) * (centd - r2 + r1) + c2.get(i) * (centd + r2 - r1)) / 2.0 / centd);
      }
      double r3 = centd + r1 + r2;
      Sphere res = new Sphere(c3, r3, s1.getPointNum() + s2.getPointNum());
      ArrayDeque<Long> points = s1.getPoints();
      if(points != null){
        for (Long p : points) {
          res.addPoint(p);
        }
      }
      points = s2.getPoints();
      if(points != null){
        for (Long p : points) {
          res.addPoint(p);
        }
      }
      res.sons.add(s1);
      res.sons.add(s2);
      return res;
    }
  }

  public static double euDist(ArrayList<Double> a, ArrayList<Double> b) throws Exception {
    if (a.size() != b.size()) {
      throw new Exception("Different dimensions.");
    }
    double dist = 0;
    for (int i = 0; i < a.size(); i++) {
      dist += Math.pow((a.get(i) - b.get(i)), 2);
    }
    dist = Math.sqrt(dist);
    return dist;
  }

  static Sphere find(Sphere sphere, Pair<Long, ArrayList<Double>> point) throws Exception {
    if (sphere.isLeaf()) {
      return sphere;
    }
    Sphere nearestSon = new Sphere(new ArrayList<>(), 0, 0);
    double minDist = Double.MAX_VALUE;
    for (Sphere son : sphere.sons) {
      double dist =
          SphereDetectionUtil.euDist(son.getCenteriod(), point.getRight()) - son.getRadium();
      if (dist < minDist) {
        minDist = dist;
        nearestSon = son;
      }
    }
    if (nearestSon.isLeaf()) {
      return nearestSon;
    } else {
      return find(nearestSon, point);
    }
  }

  static Sphere findFatherOfNearest(Sphere sphere, Pair<Long, ArrayList<Double>> point)
      throws Exception {
    if (sphere.isLeaf()) {
      return sphere;
    }
    Sphere nearestSon = new Sphere(new ArrayList<>(), 0, 0);
    double minDist = Double.MAX_VALUE;
    for (Sphere son : sphere.sons) {
      double dist =
          SphereDetectionUtil.euDist(son.getCenteriod(), point.getRight()) - son.getRadium();
      if (dist < minDist) {
        minDist = dist;
        nearestSon = son;
      }
    }
    if (nearestSon.isLeaf()) {
      return sphere;
    } else {
      return find(nearestSon, point);
    }
  }

  public static void getLeaves(Sphere sphere, ArrayList<Sphere> leaves) {
    if (sphere.isLeaf()) {
      leaves.add(sphere);
    } else {
      for (Sphere son : sphere.sons) {
        getLeaves(son, leaves);
      }
    }
  }
}
