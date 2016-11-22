/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.operations.hdfs;

import java.io.IOException;
import javax.management.MXBean;

/**
 * {@link MXBean} representing HDFS Storage statistics.
 */
public interface HDFSStorageMXBean {
  /**
   * Return the total number of bytes in HDFS.
   */
  long getTotalBytes();

  /**
   * Return the used bytes in HDFS.
   */
  long getUsedBytes();

  /**
   * Return the free bytes in HDFS.
   */
  long getRemainingBytes();

  /**
   * Return the number of missing blocks in HDFS.
   */
  long getMissingBlocks();

  /**
   * Return the number of under replicated blocks in HDFS.
   */
  long getUnderReplicatedBlocks();

  /**
   * Return the number of corrupt blocks in HDFS.
   */
  long getCorruptBlocks();
}
