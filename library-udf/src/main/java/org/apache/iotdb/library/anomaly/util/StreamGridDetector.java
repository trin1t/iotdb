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
import java.util.HashSet;

/** This class implements UDTFGridDetection. */
public class StreamGridDetector {
  private final int dimension;
  private double[] gridSize;
  private double[] origin;
  int modN = 101;
  int[] primes = new int[] {73856093, 19349663, 83492791, 10000103, 53000011};
  private final ArrayList<HashSet<Grid>> grids;
  private final HashSet<Grid> possibleAnomalyGrids;
  private int densityThreshold = 10;

  public StreamGridDetector(int dim) {
    dimension = dim;
    gridSize = new double[dimension];
    origin = new double[dimension];
    grids = new ArrayList<>();
    for (int i = 0; i < modN; i++) {
      grids.add(new HashSet<>());
    }
    possibleAnomalyGrids = new HashSet<>();
  }

  public StreamGridDetector(int dim, int thre) {
    dimension = dim;
    gridSize = new double[dimension];
    origin = new double[dimension];
    grids = new ArrayList<>();
    for (int i = 0; i < modN; i++) {
      grids.add(new HashSet<>());
    }
    possibleAnomalyGrids = new HashSet<>();
    densityThreshold = thre;
  }

  public void insert(long t, ArrayList<Double> v) {
    Grid g = findGridByCoordinate(v);
    if (g.isAnomaly()) {
      g.addPoint(t);
      possibleAnomalyGrids.add(g);
    }
  }

  public void setGridSize(double[] s) {
    gridSize = s.clone();
  }

  public void setOrigin(double[] o) {
    origin = o.clone();
  }

  private Grid findGridByCoordinate(ArrayList<Double> v) {
    ArrayList<Integer> index = new ArrayList<>();
    for (int i = 0; i < dimension; i++) {
      double cord = v.get(i) - origin[i];
      index.add(cord > 0 ? (int) (cord / gridSize[i]) : (int) (cord / gridSize[i] - 1));
    }
    int gridHash = hashGrid(index);
    for (Grid g : grids.get(gridHash)) {
      boolean flag = true;
      ArrayList<Integer> gi = g.getIndex();
      for (int i = 0; i < dimension; i++) {
        if (!index.get(i).equals(gi.get(i))) {
          flag = false;
          break;
        }
      }
      if (flag) {
        return g;
      }
    }
    Grid g = new Grid(index);
    grids.get(gridHash).add(g);
    return g;
  }

  public ArrayList<Long> flush(long lastTimestamp) {
    excludeInlierGrids();
    ArrayList<Long> anomalyTimestamps = new ArrayList<>();
    ArrayList<Grid> removeListFromPAG = new ArrayList<>();
    for (Grid grid : possibleAnomalyGrids) {
      long t = grid.getPoints().getFirst();
      while (t < lastTimestamp) {
        anomalyTimestamps.add(t);
        grid.getPoints().removeFirst();
        if (grid.getPointNum() == 1) { // no points left
          grids.get(hashGrid(grid.getIndex())).remove(grid);
          removeListFromPAG.add(grid);
          break;
        } else {
          t = grid.getPoints().getFirst();
          grid.setPointNum(grid.getPointNum() - 1);
        }
      }
    }
    for (Grid grid : removeListFromPAG) {
      possibleAnomalyGrids.remove(grid);
    }
    return anomalyTimestamps;
  }

  public void excludeInlierGrids() { // operate on possibleAnomalyGrids
    ArrayList<Grid> remmoveList = new ArrayList<>();
    for (Grid grid : possibleAnomalyGrids) {
      // dfs search
      if (!grid.isAnomaly()) {
        remmoveList.add(grid);
        continue;
      }
      HashSet<Grid> searchedGrids = new HashSet<>();
      dfsGrids(grid, 0, searchedGrids, remmoveList);
    }
    for (Grid grid : remmoveList) {
      possibleAnomalyGrids.remove(grid);
    }
  }

  public boolean dfsGrids(
      Grid grid, int points, HashSet<Grid> searched, ArrayList<Grid> removeList) {
    if (!grid.isAnomaly()) {
      return true;
    }
    searched.add(grid);
    points += grid.getPointNum();
    if (points > densityThreshold) {
      grid.setIsAnomaly(false);
      removeList.add(grid);
      return true;
    }
    ArrayList<Integer> index = new ArrayList<>(grid.getIndex());
    for (int i = 0; i < dimension; i++) {
      index.set(i, index.get(i) - 1);
      int h = hashGrid(index);
      Grid neighbour = null;
      for (Grid cand : grids.get(h)) {
        if (cand.getIndex().equals(index)) {
          neighbour = cand;
          break;
        }
      }
      if (neighbour != null) {
        if (neighbour.isAnomaly()) {
          if (!searched.contains(neighbour)) {
            if (dfsGrids(neighbour, points, searched, removeList)) {
              for (Grid inlierGrid : searched) {
                inlierGrid.setIsAnomaly(false);
                removeList.add(inlierGrid);
              }
            }
          }
        } else {
          grid.setIsAnomaly(false);
          removeList.add(grid);
          return true;
        }
      }
      index.set(i, index.get(i) + 1);
    }
    for (int i = 0; i < dimension; i++) {
      index.set(i, index.get(i) + 1);
      int h = hashGrid(index);
      Grid neighbour = null;
      for (Grid cand : grids.get(h)) {
        if (cand.getIndex().equals(index)) {
          neighbour = cand;
          break;
        }
      }
      if (neighbour != null) {
        if (neighbour.isAnomaly()) {
          if (!searched.contains(neighbour)) {
            if (dfsGrids(neighbour, points, searched, removeList)) {
              for (Grid inlierGrid : searched) {
                inlierGrid.setIsAnomaly(false);
                removeList.add(inlierGrid);
              }
            }
          }
        } else {
          grid.setIsAnomaly(false);
          removeList.add(grid);
          return true;
        }
      }
      index.set(i, index.get(i) - 1);
    }
    return false;
  }

  public ArrayList<Long> terminate() {
    excludeInlierGrids();
    ArrayList<Long> anomalyTimestamps = new ArrayList<>();
    for (Grid grid : possibleAnomalyGrids) {
      anomalyTimestamps.addAll(grid.getPoints());
    }
    return anomalyTimestamps;
  }

  private int hashGrid(ArrayList<Integer> index) {
    int hash = index.get(0) * primes[0];
    for (int i = 1; i < dimension; i++) {
      hash = hash ^ (index.get(i) * primes[i % primes.length]);
    }
    return Math.abs(hash % modN);
  }
}
