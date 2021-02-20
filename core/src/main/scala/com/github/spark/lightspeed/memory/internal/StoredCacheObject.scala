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

package com.github.spark.lightspeed.memory.internal

import com.github.spark.lightspeed.memory.{CacheValue, CacheValueStats, EvictionManager, TransformValue}

/**
 * Base class for key+value pairs stored in the in-memory cache by [[EvictionManager]].
 *
 * @param key The key for the object which should be the same as that provided in
 *            [[EvictionManager]] API methods like `putObject`, `getDecompressed` etc.
 * @param value The [[CacheValue]] instance provided in [[EvictionManager.putObject]] and by
 *              loader in [[EvictionManager.getDecompressed]].
 * @param transformer The [[TransformValue]] implementation for the `value` provided in
 *                    [[EvictionManager.putObject]] and by loader in
 *                    [[EvictionManager.getDecompressed]].
 * @param initialWeightage The initial `weightage` for this object which is usually determined
 *                         using the current timestamp.
 *
 * @tparam T the concrete type of [[CacheValue]] contained in this object
 */
sealed abstract class StoredCacheObject[T <: CacheValue](
    final val key: Comparable[_ <: AnyRef],
    final val value: T,
    final val transformer: TransformValue[_ <: CacheValue, _ <: CacheValue],
    initialWeightage: Double) {

  /**
   * The underlying field for [[weightage]].
   */
  private[this] final var _weightage: Double = initialWeightage

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for eviction. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] final def weightage: Double = _weightage

  /**
   * Setter for [[weightage]] that will also update [[weightageWithoutBoost]].
   */
  private[memory] final def weightage_=(value: Double): Unit = {
    _weightage = value
    _weightageWithoutBoost = value
  }

  /**
   * Indicates the number of times a stored object was "resurrected" i.e. evicted and then cached
   * again. The higher this number, the more it will add to [[weightage]] with the expectation
   * that there is a higher likelihood of the object being faulted in again. Usually this will
   * be done by [[EvictionManager]] implementations by maintaining statistics for evicted objects
   * and adding those to the [[weightage]] when an object is "resurrected" in cache.
   *
   * The second purpose of this is to compare objects within the same or older [[generation]]s.
   * For example, a scan is going to read disk blocks that includes both higher and lower
   * [[weightage]] objects out of which only the higher ones are in cache due to a previous
   * similar scan. But the scan reads the lower [[weightage]] objects first and tries to cache
   * them which leads to eviction of otherwise higher [[weightage]] ones because of boosted
   * [[weightage]] of the lower ones due to current access. To avoid this cycle from repeating
   * multiple times, when comparing objects within the same or older non-zero [[generation]],
   * the most recent access will be ignored and then the [[weightage]] compared.
   *
   * One danger in above is out-of-date field i.e. the last caching/eviction for the object
   * happened a long time in the past. To avoid this, if the overall [[weightage]] has gone
   * down significantly, then the [[EvictionManager]] manager can decide to completely remove
   * these statistics for the evicted objects.
   */
  private[memory] final var generation: Int = _

  /**
   * Keep the [[weightage]] without boost instead of the boost itself so that readers can safely
   * access the value without locks. Otherwise performing (weightage - boost) would not be atomic
   * without having locks for both writers and readers. With this tracked separately, readers
   * will read a consistent value in [[weightageWithoutBoost]] either before or after update.
   */
  private[this] final var _weightageWithoutBoost: Double = initialWeightage

  /**
   * The [[weightage]] without the artificial "boost" added by [[addGenerationalBoost]].
   */
  private[memory] final def weightageWithoutBoost: Double = _weightageWithoutBoost

  /**
   * An artificial "boost" given to a "resurrected" object when it is determined to have a high
   * likelihood of being accessed in the current scan as noted in second point of [[generation]].
   * This is added to [[weightage]] temporarily and adjusted back when the actual access is done.
   * Note that this should be taken into account in comparison only for objects that are of the
   * same or higher [[generation]]. Additionally this should be cleared if the access is not
   * performed in some timeframe.
   */
  private[memory] final def addGenerationalBoost(boost: Double): Unit = {
    _weightage += boost
  }

  /**
   * Returns true if a non-zero "boost" was previously applied using [[addGenerationalBoost]].
   */
  private[memory] final def hasGenerationalBoost: Boolean = _weightage != _weightageWithoutBoost

  /**
   * Returns true if this object is compressed and false otherwise. If the [[CacheValue]]
   * does not support compression (i.e. [[TransformValue.compressionAlgorithm]] is [[None]]),
   * then this should be false.
   */
  def isCompressed: Boolean

  /**
   * Get the bare minimum statistics for this cached object.
   */
  def toStats: CacheValueStats
}

/**
 * Implementation of decompressed [[StoredCacheObject]]s. Apart from overridden methods,
 * this contains a `compress` method used by [[EvictionManager]] which uses the
 * [[TransformValue]] that was provided while constructing this object.
 *
 * @tparam D the type of contained decompressed object
 * @tparam C the type of compressed object obtained after compression
 */
final class DecompressedCacheObject[D <: CacheValue, C <: CacheValue](
    _key: Comparable[_ <: AnyRef],
    _value: D,
    _transformer: TransformValue[C, D],
    _weightage: Double)
    extends StoredCacheObject[D](_key, _value, _transformer, _weightage) {

  override def isCompressed: Boolean = false

  override def toStats: CacheValueStats = new CacheValueStats(weightageWithoutBoost, generation)

  /**
   * Compress the given object (should be a hard reference to the contained object) and return
   * [[CompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  def compress(
      value: D,
      timeWeightage: Double,
      decompressionToDiskReadCost: Double): CompressedCacheObject[C, D] = {
    val transformer = this.transformer.asInstanceOf[TransformValue[C, D]]
    val compressedVal = transformer.compress(value)
    val compressionSavings = Utils.calcCompressionSavings(
      timeWeightage,
      decompressionToDiskReadCost,
      compressedVal.memorySize,
      value.memorySize)
    val compressed = new CompressedCacheObject[C, D](
      key,
      compressedVal,
      transformer,
      weightageWithoutBoost + compressionSavings,
      compressionSavings)
    compressed.generation = generation
    compressed
  }
}

/**
 * Implementation of compressed [[StoredCacheObject]]s. Apart from overridden methods,
 * this contains a `decompress` method used by [[EvictionManager]] which uses the
 * [[TransformValue]] that was provided while constructing this object.
 *
 * @param compressionSavings If the [[CacheValue]] is compressed, then any extra savings due to
 *                           compression accounted separately in the `weightage`. This is used to
 *                           adjust the `weightage` when creating its decompressed version.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
final class CompressedCacheObject[C <: CacheValue, D <: CacheValue](
    _key: Comparable[_ <: AnyRef],
    _value: C,
    _transformer: TransformValue[C, D],
    _weightage: Double,
    private[memory] val compressionSavings: Double)
    extends StoredCacheObject[C](_key, _value, _transformer, _weightage) {

  override def isCompressed: Boolean = true

  override def toStats: CacheValueStats =
    new CacheValueStats(weightageWithoutBoost - compressionSavings, generation)
}
