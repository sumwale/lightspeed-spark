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
trait EvictionManager[C <: CacheValue, D <: CacheValue] {

  /**
   * Set/reset the upper limit on the memory (heap or off-heap) in bytes. If the current usage
   * exceeds the given limit, then objects should be evicted from cache immediately as required.
   *
   * @param newMaxMemory the new upper limit for the memory in bytes
   * @param timestamp the current timestamp which should be the [[System.currentTimeMillis()]]
   *                  at the start of operation
   */
  def setLimit(newMaxMemory: Long, timestamp: Long): Unit

  /**
   * Add a compressed/decompressed object to the cache. Note that only the fields of the passed
   * object are used and the `stored` field is expected to be None while others are expected to
   * be non-null.
   *
   * @param obj the object to be put in the cache; note that it is converted to a
   *            [[StoredCacheObject]] by [[EvictionManager]] for storage; all the fields
   *            of the provided object are required to be valid/non-null except the `stored`
   *            field which is for internal used and should always be [[None]] for this operation
   * @param timestamp the current [[System.currentTimeMillis()]]; this has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert
   *
   * @tparam T if the object is compressed then should be same as `C` else as `D`
   * @tparam U if the object is compressed then should be same as `D` else as `C`
   *
   * @return true if the put succeeded and false if the put failed due to either no memory being
   *         available or if the same [[PublicCacheObject.key]] is already present in the cache
   */
  def putObject[T <: CacheValue, U <: CacheValue](
      obj: PublicCacheObject[T, U],
      timestamp: Long): Boolean

  /**
   * Get the decompressed version of the object with its wrapper for given key. The provided key
   * object should be same as [[CacheObject.key]] of returned wrapper for consistency.
   *
   * @param key the key to lookup the object which should be the same as [[PublicCacheObject.key]]
   *            field in the [[putObject]] operation
   * @param timestamp the current [[System.currentTimeMillis()]]; this has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert;
   *                  this is used internally by [[EvictionManager]] for cases where the
   *                  decompressed object is determined to better be cached
   */
  def getDecompressed(key: Comparable[AnyRef], timestamp: Long): Option[PublicCacheObject[D, C]]
}
