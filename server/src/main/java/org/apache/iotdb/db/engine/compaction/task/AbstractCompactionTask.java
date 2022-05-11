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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.RewriteCrossCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.inner.sizetiered.SizeTieredCompactionRecoverTask;
import org.apache.iotdb.db.service.metrics.Metric;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Tag;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it execute the abstract function
 * <i>doCompaction</i> implemented by subclass, and decrease the currentTaskNum in
 * CompactionScheduler when the <i>doCompaction</i> finish.
 */
public abstract class AbstractCompactionTask implements Callable<Void> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected String fullStorageGroupName;
  protected long timePartition;
  protected final AtomicInteger currentTaskNum;

  public AbstractCompactionTask(
      String fullStorageGroupName, long timePartition, AtomicInteger currentTaskNum) {
    this.fullStorageGroupName = fullStorageGroupName;
    this.timePartition = timePartition;
    this.currentTaskNum = currentTaskNum;
  }

  public abstract void setSourceFilesToCompactionCandidate();

  protected abstract void doCompaction() throws Exception;

  @Override
  public Void call() throws Exception {
    long startTime = System.currentTimeMillis();
    currentTaskNum.incrementAndGet();
    try {
      doCompaction();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    } finally {
      if (!(this instanceof RewriteCrossCompactionRecoverTask)
          && !(this instanceof SizeTieredCompactionRecoverTask)) {
        CompactionTaskManager.getInstance().removeRunningTaskFromList(this);
      }
      this.currentTaskNum.decrementAndGet();
    }

    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      MetricsService.getInstance()
          .getMetricManager()
          .timer(
              System.currentTimeMillis() - startTime,
              TimeUnit.MILLISECONDS,
              Metric.COST_TASK.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              "compaction");
    }

    return null;
  }

  public String getFullStorageGroupName() {
    return fullStorageGroupName;
  }

  public long getTimePartition() {
    return timePartition;
  }

  public abstract boolean equalsOtherTask(AbstractCompactionTask otherTask);

  /**
   * Check if the compaction task is valid (selected files are not merging, closed and exist). If
   * the task is valid, then set the merging status of selected files to true.
   *
   * @return true if the task is valid else false
   */
  public abstract boolean checkValidAndSetMerging();

  @Override
  public boolean equals(Object other) {
    if (other instanceof AbstractCompactionTask) {
      return equalsOtherTask((AbstractCompactionTask) other);
    }
    return false;
  }

  public abstract void resetCompactionCandidateStatusForAllSourceFiles();
}
