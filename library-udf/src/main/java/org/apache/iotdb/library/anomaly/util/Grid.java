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

import java.util.ArrayDeque;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Grid used in StreamGridDetector.
 */
public class Grid {
  private final ArrayList<Integer> index; // record this is n-th grid on each axis
  private ArrayList<Pair<Double, Double>> border; // record the border of the grid
  private int pointNum;
  private ArrayDeque<Long> points;
  private boolean isAnomaly;

  public Grid(ArrayList<Integer> i){
    index = new ArrayList<>(i);
    pointNum = 0;
    points = new ArrayDeque<>();
    isAnomaly = true;
  }
  public Grid(ArrayList<Integer> i, int pnum){
    index = new ArrayList<>(i);
    pointNum = pnum;
    points = new ArrayDeque<>();
    isAnomaly = true;
  }
  public Grid(ArrayList<Integer> i, ArrayList<Pair<Double, Double>> b){
    index = new ArrayList<>(i);
    border = b;
    pointNum = 0;
    points = new ArrayDeque<>();
    isAnomaly = true;
  }
  public Grid(ArrayList<Integer> i, ArrayList<Pair<Double, Double>> b, int pnum){
    index = new ArrayList<>(i);
    border = b;
    pointNum = pnum;
    isAnomaly = true;
    points = new ArrayDeque<>(); {
    };
  }
  public void addPoint(long t){
    points.addLast(t);
    pointNum ++;
  }

  public ArrayList<Integer> getIndex(){
    return index;
  }

  public void setIsAnomaly(boolean a){
    isAnomaly = a;
    if(!isAnomaly){
      points = null;
    }
  }

  public boolean isAnomaly() {
    return isAnomaly;
  }

  public int getPointNum() {
    return pointNum;
  }

  public ArrayDeque<Long> getPoints(){
    return points;
  }

  public void setPointNum(int n){
    pointNum = n;
  }
}
