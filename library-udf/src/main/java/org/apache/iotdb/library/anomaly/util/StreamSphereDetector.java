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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/** This class implements UDTFSphereDetection */
public class StreamSphereDetector {
  public Sphere root;
  private ArrayList<Pair<Long, ArrayList<Double>>> PossibleOutliers = new ArrayList<>();
  public double densityThreshold;
  public int regenerateThreshold;

  public StreamSphereDetector(double d, int r) {
    densityThreshold = d;
    regenerateThreshold = r;
  }

  public void initializeTree(ArrayList<Pair<Long, ArrayList<Double>>> points) throws Exception {
    ArrayList<ArrayList<Double>> dists = new ArrayList<>();
    for (int i = 0; i < points.size() - 1; i++) {
      dists.add(new ArrayList<>());
      for (int j = i + 1; j < points.size(); j++) {
        dists.get(i).add(0d);
      }
    }
    HashMap<Integer, Sphere> spheres = new HashMap<>();
    for (int i = 0; i < points.size(); i++) {
      Sphere tempSphere = new Sphere(points.get(i).getRight(), 0, 0);
      tempSphere.addPoint(points.get(i).getLeft());
      spheres.put(i, tempSphere);
    }
    for (int i = 0; i < points.size() - 1; i++) {
      for (int j = i + 1; j < points.size(); j++) {
        double d = SphereDetectionUtil.centerDist(spheres.get(i), spheres.get(j));
        dists.get(i).set(j - i - 1, d);
      }
    }
    while (spheres.size() > 1) {
      double minDist = Double.MAX_VALUE;
      int minI = -1, minJ = -1;
      Set<Integer> keys = spheres.keySet();
      for (Integer i : keys) {
        for (Integer j : keys) {
          if (i < j && dists.get(i).get(j - i - 1) < minDist) {
            minDist = dists.get(i).get(j - i - 1);
            minI = i;
            minJ = j;
          }
        }
      }
      ArrayList<Sphere> meregList = new ArrayList<>();
      Sphere newSphere = SphereDetectionUtil.merge(spheres.get(minI), spheres.get(minJ));
      spheres.remove(minJ);
      spheres.put(minI, newSphere);
      for (Integer j : spheres.keySet()) {
        if (minI > j) {
          dists.get(j).set(minI - j - 1, SphereDetectionUtil.centerDist(newSphere, spheres.get(j)));
        } else if (minI < j) {
          dists
              .get(minI)
              .set(j - minI - 1, SphereDetectionUtil.centerDist(newSphere, spheres.get(j)));
        }
      }
      keys = spheres.keySet();
      ArrayList<Integer> removeKeys = new ArrayList<>();
      for (Integer key : keys) {
        if (key < minI) {
          if (dists.get(key).get(minI - key - 1)
              <= Math.abs(spheres.get(key).getRadium() - newSphere.getRadium())) {
            meregList.add(spheres.get(key));
            removeKeys.add(key);
          }
        } else if (key > minI) {
          if (dists.get(minI).get(key - minI - 1)
              <= Math.abs(spheres.get(key).getRadium() - newSphere.getRadium())) {
            meregList.add(spheres.get(key));
            removeKeys.add(key);
          }
        }
      }
      for (Integer key : removeKeys) {
        spheres.remove(key);
      }
      newSphere.sons.addAll(meregList);
      if (newSphere.get1dDensity() > densityThreshold) {
        newSphere.setIsAnomaly(false);
        newSphere.sons.clear();
      } else {
        ArrayList<Sphere> removeList = new ArrayList<>();
        for (Sphere s : newSphere.sons) {
          if (s.getRadium() == 0) {
            PossibleOutliers.add(Pair.of(s.points.getFirst(), s.getCenteriod()));
            removeList.add(s);
          }
        }
        for (Sphere s : removeList) {
          newSphere.sons.remove(s);
        }
      }
    }
    for (Sphere s : spheres.values()) {
      root = s;
    }
  }

  public ArrayList<Long> flush(ArrayList<Pair<Long, ArrayList<Double>>> points) throws Exception {
    ArrayList<Pair<Long, ArrayList<Double>>> newPossibleOutliers = new ArrayList<>();
    ArrayList<Long> res = new ArrayList<>();
    for (Pair<Long, ArrayList<Double>> p : points) {
      Sphere nearestFather = SphereDetectionUtil.findFatherOfNearest(root, p);
      Sphere nearest = SphereDetectionUtil.find(nearestFather, p);
      if (nearest.contain(p)) {
        nearest.setPointNum(nearest.getPointNum() + 1);
      } else {
        Sphere newSphere = SphereDetectionUtil.merge(new Sphere(p.getRight(), 0, 1), nearest);
        if (newSphere.get1dDensity() > densityThreshold) {
          newSphere.sons.clear();
          nearestFather.sons.remove(nearest);
          nearestFather.sons.add(newSphere);
        } else {
          newSphere.sons.removeIf(s -> s.getRadium() == 0);
          newPossibleOutliers.add(p);
        }
      }
    }
    for (Pair<Long, ArrayList<Double>> p : PossibleOutliers) {
      Sphere nearestFather = SphereDetectionUtil.findFatherOfNearest(root, p);
      Sphere nearest = SphereDetectionUtil.find(nearestFather, p);
      if (nearest.contain(p)) {
        nearest.setPointNum(nearest.getPointNum() + 1);
      } else {
        Sphere newSphere = SphereDetectionUtil.merge(new Sphere(p.getRight(), 0, 1), nearest);
        if (newSphere.get1dDensity() > densityThreshold) {
          newSphere.sons.clear();
          nearestFather.sons.remove(nearest);
          nearestFather.sons.add(newSphere);
        } else {
          newSphere.sons.removeIf(s -> s.getRadium() == 0);
          res.add(p.getLeft());
        }
      }
    }
    ArrayList<Pair<Long, ArrayList<Double>>> allPossibleOutliers = new ArrayList<>();
    if (res.size() + newPossibleOutliers.size() > regenerateThreshold) {
      int j = 0;
      for (int i = 0; i < res.size(); i++) {
        while (!PossibleOutliers.get(j).getLeft().equals(res.get(i))) {
          j++;
        }
        allPossibleOutliers.add(PossibleOutliers.get(j));
      }
      allPossibleOutliers.addAll(newPossibleOutliers);
      StreamSphereDetector newDetector =
          new StreamSphereDetector(densityThreshold, regenerateThreshold);
      newDetector.initializeTree(allPossibleOutliers);
      newPossibleOutliers = newDetector.PossibleOutliers;
      ArrayList<Sphere> leaves = new ArrayList<>();
      SphereDetectionUtil.getLeaves(root, leaves);
      SphereDetectionUtil.getLeaves(root, leaves);
      createTree(leaves);
      res = new ArrayList<>();
      for (Pair<Long, ArrayList<Double>> p : PossibleOutliers) {
        res.add(p.getLeft());
      }
    }
    PossibleOutliers = newPossibleOutliers;
    return res;
  }

  public void createTree(ArrayList<Sphere> sphereList) throws Exception {
    if (sphereList.size() == 1) {
      root = sphereList.get(0);
    }
    ArrayList<ArrayList<Double>> dists = new ArrayList<>();
    for (int i = 0; i < sphereList.size() - 1; i++) {
      dists.add(new ArrayList<>());
      for (int j = i + 1; j < sphereList.size(); j++) {
        dists.get(i).add(0d);
      }
    }
    HashMap<Integer, Sphere> spheres = new HashMap<>();
    for (int i = 0; i < sphereList.size(); i++) {
      spheres.put(i, sphereList.get(i));
    }
    for (int i = 0; i < spheres.size() - 1; i++) {
      for (int j = i + 1; j < spheres.size(); j++) {
        double d = SphereDetectionUtil.centerDist(spheres.get(i), spheres.get(j));
        dists.get(i).set(j - i - 1, d);
      }
    }
    while (spheres.size() > 1) {
      double minDist = Double.MAX_VALUE;
      int minI = -1, minJ = -1;
      Set<Integer> keys = spheres.keySet();
      for (Integer i : keys) {
        for (Integer j : keys) {
          if (i < j && dists.get(i).get(j - i - 1) < minDist) {
            minDist = dists.get(i).get(j - i - 1);
            minI = i;
            minJ = j;
          }
        }
      }
      ArrayList<Sphere> meregList = new ArrayList<>();
      Sphere newSphere = SphereDetectionUtil.merge(spheres.get(minI), spheres.get(minJ));
      spheres.remove(minJ);
      spheres.put(minI, newSphere);
      for (Integer j : spheres.keySet()) {
        if (minI > j) {
          dists.get(j).set(minI - j - 1, SphereDetectionUtil.centerDist(newSphere, spheres.get(j)));
        } else if (minI < j) {
          dists
              .get(minI)
              .set(j - minI - 1, SphereDetectionUtil.centerDist(newSphere, spheres.get(j)));
        }
      }
      keys = spheres.keySet();
      ArrayList<Integer> removeKeys = new ArrayList<>();
      for (Integer key : keys) {
        if (key < minI) {
          if (dists.get(key).get(minI - key - 1)
              <= Math.abs(spheres.get(key).getRadium() - newSphere.getRadium())) {
            meregList.add(spheres.get(key));
            removeKeys.add(key);
          }
        } else if (key > minI) {
          if (dists.get(minI).get(key - minI - 1)
              <= Math.abs(spheres.get(key).getRadium() - newSphere.getRadium())) {
            meregList.add(spheres.get(key));
            removeKeys.add(key);
          }
        }
      }
      for (Integer key : removeKeys) {
        spheres.remove(key);
      }
      newSphere.sons.addAll(meregList);
      if (newSphere.get1dDensity() > densityThreshold) {
        newSphere.setIsAnomaly(false);
        newSphere.sons.clear();
      } else {
        ArrayList<Sphere> removeList = new ArrayList<>();
        for (Sphere s : newSphere.sons) {
          if (s.getRadium() == 0) {
            PossibleOutliers.add(Pair.of(s.points.getFirst(), s.getCenteriod()));
            removeList.add(s);
          }
        }
        for (Sphere s : removeList) {
          newSphere.sons.remove(s);
        }
      }
    }
    for (Sphere s : spheres.values()) {
      root = s;
    }
  }

  public ArrayList<Pair<Long, ArrayList<Double>>> getPossibleOutliers() {
    return PossibleOutliers;
  }
}
