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

/**
 * Sphere to construct ball tree.
 */
public class Sphere {
  private ArrayList<Double> centeriod; // record this is n-th grid on each axis
  private double radium;
  private int pointNum;
  public ArrayDeque<Long> points;
  private boolean isAnomaly;

  public Sphere(ArrayList<Double> o, double r, int p){
    centeriod = new ArrayList<>(o);
    radium = r;
    pointNum = p;
    points = new ArrayDeque<>();
    isAnomaly = true;
  }

  public void addPoint(long t){
    points.addLast(t);
    pointNum ++;
  }

  public ArrayList<Double> getCenteriod(){
    return centeriod;
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

  public void setRadium(double r){
    radium = r;
  }

  public double getRadium(){return radium;}

  public void setCenteriod(ArrayList<Double> o){
    centeriod = new ArrayList<>(o);
  }
}
