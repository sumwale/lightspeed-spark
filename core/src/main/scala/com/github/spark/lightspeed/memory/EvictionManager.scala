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

package com.github.spark.lightspeed.memory

/**
 * Interface for a generic manager of in-memory objects that need to be evicted when running low
 * on memory. Two kinds of objects namely compressed and decompressed should be supported by
 * implementations which should balance between the two as per the determined `cost` (e.g.
 * compression of objects allows keeping more objects in memory while incurring the overhead of
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

  type StatisticsMap = scala.collection.Map[Comparable[_ <: AnyRef], CacheValueStats]

  /**
   * Set/reset the upper limit on the memory (heap or off-heap) in bytes. If the current usage
   * exceeds the given limit, then objects should be evicted from cache immediately to honor the
   * new limit.
   *
   * @param newMaxMemory the new upper limit for the memory in bytes
   * @param timestamp the current timestamp which should be the [[System.currentTimeMillis]]
   *                  at the start of operation
   */
  def setLimit(newMaxMemory: Long, timestamp: Long): Unit

  /**
   * Add a compressed/decompressed object to the cache. Note that the fields of the passed object
   * are expected to be non-null and valid.
   *
   * A successful put operation does not mean a whole lot in most cases because the object that
   * was put could be evicted soon after by other puts/loads. Using a loader in [[getDecompressed]]
   * should be preferred over explicit puts since it works transparently and is easier to use.
   *
   * @param key A unique key for the object. This object cannot be same as the value itself or
   *            contain a reference to value since this can be retained for long-term statistics.
   * @param either The object to be put in the cache. Note that it is converted to a form
   *               appropriate for storage by [[EvictionManager]]. All the fields of the provided
   *               object are required to be valid/non-null. If the [[Either]] resolves to
   *               [[Left]], then its [[CacheValue.isCompressed]] should be true else false.
   * @param transformer Implementation to compress/decompress the provided `value` and create
   *                    `finalizer` for release. Typically this should have static implementations
   *                    (e.g. per `compressionAlgorithm`) to minimize the objects held in cache.
   * @param timestamp The current [[System.currentTimeMillis]]. This has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert.
   *
   * @return true if the value was put into the cache (though can be evicted soon after) and false
   *         if caching was skipped due to too large a size or small `weightage`
   *
   * @throws IllegalArgumentException if [[TransformValue.compressionAlgorithm]] is None but
   *                                  [[CacheValue.isCompressed]] is true
   * @throws IllegalArgumentException if [[Either]] argument is [[Left]] but
   *                                  [[CacheValue.isCompressed]] is false or it is true and the
   *                                  argument is [[Right]]
   * @throws IllegalArgumentException if `key` or `transformer` or the `transformer's`
   *                                  `compressionAlgorithm` is null or value's `memorySize` is -ve
   * @throws UnsupportedOperationException if the given object's memorySize is greater than the
   *                                       [[EvictionManager]]'s maximum limit itself
   */
  def putObject(
      key: Comparable[_ <: AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long): Boolean

  /**
   * Get the decompressed version of the object for given key.
   * If [[TransformValue.compressionAlgorithm]] is [[None]], then there is no compression for
   * the [[CacheValue]] so the value as put in [[putObject]] or returned by `loader` is returned
   * (and it is required that [[CacheValue.isCompressed]] is false for both cases).
   *
   * The return value, if found in cache or returned by loader, will have its reference count
   * incremented (i.e. equivalent of [[CacheValue.use]]) so callers should do an explicit
   * `release` when done with the object.
   *
   * @param key The key to lookup the object which should be the same as the one provided in
   *            the [[putObject]] operation.
   * @param timestamp The current [[System.currentTimeMillis]]. This has been provided
   *                  as an argument with the expectation that a single common timestamp
   *                  will be used for all objects that are part of a single operation so
   *                  that there is no relative priority among objects of the same scan/insert.
   * @param loader In case the value is not found in cache, then this can be optionally provided
   *               to load the object with its associated [[TransformValue]] and possibly put
   *               into cache before returning. If the result resolves to [[Left]], then its
   *               [[CacheValue.isCompressed]] should be true else false.
   *
   * @return The decompressed object from cache or loader, if present else [[None]]. In the former
   *         case the object will have its reference count incremented.
   */
  def getDecompressed(
      key: Comparable[_ <: AnyRef],
      timestamp: Long,
      loader: Option[Comparable[_ <: AnyRef] => Option[(Either[C, D], TransformValue[C, D])]])
    : Option[D]

  /**
   * Get [[CacheValueStats]] for the keys that satisfy the given `predicate`.
   *
   * @param predicate function taking a key and returning true if [[CacheValueStats]] should be
   *                  included in the result
   *
   * @return map of keys to their corresponding [[CacheValueStats]]
   */
  def getStatistics(predicate: Comparable[_ <: AnyRef] => Boolean): StatisticsMap

  /**
   * Put a list of statistics into the [[EvictionManager]] as extracted using [[getStatistics]].
   * This will be stored internally and will be applied when the values for those keys are
   * actually cached by [[getDecompressed]] or [[putObject]].
   *
   * @param statistics map of statistics as returned by [[getStatistics]]
   */
  def putStatistics(statistics: StatisticsMap): Unit

  /**
   * Remove the given key from cache and report if the operation was successful. This also removes
   * any statistics recorded against the key so should be only used if the key is really gone.
   *
   * @param key The key to lookup the object which should be the same as the one provided in
   *            the [[putObject]] and [[getDecompressed]] operations.
   *
   * @return true if the key was present in cache and removed, and false otherwise
   */
  def removeObject(key: Comparable[_ <: AnyRef]): Boolean

  /**
   * Remove all the entries in cache, including their statistics, for which the given predicate
   * is true. This can be used, for example, to remove all entries of a table when it is dropped.
   *
   * @param predicate function taking a key and returning true if the entry should be removed
   *
   * @return number of cached entries that were removed (excludes statistics-only removals)
   */
  def removeAll(predicate: Comparable[_ <: AnyRef] => Boolean): Int

  /**
   * For cases where any of the other methods have thrown unexpected errors like OutOfMemory,
   * the cache count and maps can potentially go out of sync. This method can be explicitly
   * invoked to check and bring the cache into a consistent shape.
   *
   * @return Error messages (which are also logged as errors) if fixes were required to the cache
   *         or empty if everything was consistent.
   */
  def checkAndForceConsistency(): Seq[String]
}
