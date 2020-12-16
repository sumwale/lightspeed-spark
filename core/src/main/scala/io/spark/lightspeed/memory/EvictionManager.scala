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
 * Implementations usually should be indifferent to the type of memory used by the managed
 * [[CacheValue]] objects that can be heap or off-heap. For latter case additional modules
 * can handle cases of real available RAM running low, in which case they can invoke the
 * `setLimit` method dynamically as required (and [[EvictionManager]] is required to evict
 * immediately to enforce the new limit synchronously blocking other operations as required).
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
   * @param key A unique key for the object. This object cannot be same as the value itself or
   *            contain a reference to value since this can be retained for long-term statistics.
   * @param either The object to be put in the cache. Note that it is converted to a form
   *               appropriate for storage by [[EvictionManager]]; all the fields of the provided
   *               object are required to be valid/non-null; if the [[Either]] resolves to
   *               [[Left]], then its [[CacheValue.isCompressed]] should be true else false.
   * @param transformer Implementation to compress/decompress the provided `value` and `finalize`
   *                    it for release. Typically this should have static implementations
   *                    (e.g. per `compressionAlgorithm`) to minimize the objects held in cache.
   * @param timestamp The current [[System.currentTimeMillis()]]. This has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert.
   *
   * @return True if the put succeeded and false if the put failed due to either no memory being
   *         available or if the same `key` is already present in the cache.
   */
  def putObject(
      key: Comparable[AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long): Boolean

  /**
   * Get the decompressed version of the object for given key.
   * If [[TransformValue.compressionAlgorithm]] is [[None]], then there is no compression for
   * the [[CacheValue]] so the value as put in [[putObject]] or returned by `loader` is returned
   * (and it is required that [[CacheValue.isCompressed]] is false for both).
   *
   * @param key The key to lookup the object which should be the same as the one provided in
   *            the [[putObject]] operation.
   * @param timestamp The current [[System.currentTimeMillis()]]. This has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert;
   *                  this is used internally by [[EvictionManager]] for cases where the
   *                  decompressed object is cached transparently.
   * @param loader In case the value is not found in cache, then this can be optionally provided
   *               to load the object with its associated [[TransformValue]] and possibly put
   *               into cache before returning. If the result resolves to [[Left]], then its
   *               [[CacheValue.isCompressed]] should be true else false.
   */
  def getDecompressed(
      key: Comparable[AnyRef],
      timestamp: Long,
      loader: Option[Comparable[AnyRef] => Option[(Either[C, D], TransformValue[C, D])]])
    : Option[D]
}
