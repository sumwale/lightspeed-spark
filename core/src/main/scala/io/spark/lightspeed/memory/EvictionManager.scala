/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.spark.lightspeed.memory

/**
 * Interface for a generic manager for in-memory objects that need to be evicted when running low
 * on memory. Two kinds of objects namely compressed and decompressed should be supported by
 * implementations which should balance between the two as per the determined `cost` (e.g.
 * compressed objects allows keeping more objects in memory while incurring the overhead of
 * decompression so a balance has to be achieved between the two depending on available memory).
 * Implementations can apply various policies like LRU, LFU, FIFO or combinations.
 * Implementations usually should be indifferent to the type of memory used by the managed objects
 * (contained within [[CacheObject]]) that can be heap or off-heap. For latter case additional
 * modules can handle cases of real available RAM running low, in which case they can invoke
 * the `setLimit` method dynamically as required (and [[EvictionManager]]s are required to evict
 * immediately to honour).
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
trait EvictionManager[C, D] {

  /**
   * Set/reset the upper limit on the memory (heap or off-heap). If the current usage exceeds
   * the given limit, then objects should be evicted from cache immediately as much required.
   */
  def setLimit(maxMemorySize: Long): Unit

  /**
   * Add a compressed object to the cache. Note that decompression will be taken care of
   * transparently by the [[EvictionManager]] as per its own policy and should not be done
   * by the caller (which should use the [[getDecompressed]] method instead).
   */
  def putCompressed(obj: CompressedCacheObject[C, D]): Boolean

  /**
   * Get the compressed version of the object for given key. The provided key object should
   * be same as that that returned by resulting object's [[CacheObject.key]] for consistency.
   */
  def getCompressed(key: Comparable[AnyRef]): Option[CompressedCacheObject[C, D]]

  /**
   * Get the decompressed version of the object for given key. The provided key object should
   * be same as that that returned by resulting object's [[CacheObject.key]] for consistency.
   */
  def getDecompressed(key: Comparable[AnyRef]): Option[DecompressedCacheObject[D, C]]
}
