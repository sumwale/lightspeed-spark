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

import com.github.spark.lightspeed.memory.{CacheValue, EvictionManager, TransformValue}

/**
 * Base class for key+value pairs stored in the in-memory cache by [[EvictionManager]].
 *
 * @tparam T the type of value contained in this object which is cleared on eviction
 */
sealed abstract class StoredCacheObject[T <: CacheValue](
    /**
     * The key for the object which should be the same as that provided in [[EvictionManager]]'s
     * methods like `putObject`, `getDecompressed` etc.
     */
    final val key: Comparable[AnyRef],
    /**
     * The [[CacheValue]] instance provided in [[EvictionManager.putObject]] and by loader
     * in [[EvictionManager.getDecompressed]]. This can be `null` if this object has been
     * evicted from cache.
     */
    final val value: T,
    /**
     * The [[TransformValue]] implementation for the `value` provided in
     * [[EvictionManager.putObject]] and by loader in [[EvictionManager.getDecompressed]].
     */
    final val transformer: TransformValue[_ <: CacheValue, _ <: CacheValue]) {

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for evictions. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] final var weightage: Double = _

  /**
   * Indicates the number of times a stored object was "resurrected" i.e. evicted and then cached
   * again. The higher this number, the more it will add to [[weightage]] with the expectation
   * that there is a higher likelihood of the object being faulted in again. [[EvictionManager]]
   * implementations can use this field with [[weightage]] to keep some minimal statistics for
   * otherwise evicted objects for some period of time.
   *
   * The second purpose of this is to compare objects within the same [[generation]].
   * For example, a scan is going to read disk blocks that includes both higher and lower
   * [[weightage]] objects out of which only the higher ones are in cache due to a previous
   * similar scan. But the scan reads the lower [[weightage]] objects first and tries to cache
   * them which leads to eviction of otherwise higher [[weightage]] ones because of boosted
   * [[weightage]] of the lower ones due to current access. To avoid this cycle from repeating
   * multiple times, when comparing objects within the same non-zero [[generation]], the most
   * recent access will be ignored and then the [[weightage]] compared.
   *
   * One danger in above is out-of-date field i.e. the last caching/eviction for the object
   * happened a long time in the past. To avoid this, if the overall [[weightage]] has gone
   * down significantly, then the [[EvictionManager]] manager can decide to completely remove
   * these statistics for the evicted objects.
   */
  private[memory] final var generation: Int = _

  /**
   * The artificial "boost" given to a "resurrected" object when it is determined to have a high
   * likelihood of being accessed in the current scan as noted in second point of [[generation]].
   * This is added to [[weightage]] temporarily and adjusted back when the actual access is done.
   * Note that this should be taken into account in comparison only for objects that are of the
   * same or higher [[generation]]. Additionally this should be cleared if the access is not
   * performed in some timeframe.
   */
  private[memory] final var generationalBoost: Double = _

  /**
   * Returns true if this object is compressed and false otherwise. If the [[CacheValue]]
   * does not support compression (i.e. [[TransformValue.compressionAlgorithm]] is [[None]]),
   * then this should be false.
   */
  def isCompressed: Boolean

  /**
   * Get the [[weightage]] removing the temporary addition of [[generationalBoost]] if any.
   */
  def weightageWithoutBoost: Double =
    if (generationalBoost > 0.0) weightage - generationalBoost else weightage

  /**
   * Get the bare minimum statistics for this cached object.
   */
  def toStats: CacheValueStats
}

object StoredCacheObject {

  /**
   * Create an appropriate [[StoredCacheObject]] for given [[CacheValue]] and [[TransformValue]].
   * This should only be used by [[EvictionManager]] to transform the arguments of
   * [[EvictionManager.putObject]] for storage in the cache.
   */
  def apply[C <: CacheValue, D <: CacheValue](
      key: Comparable[AnyRef],
      value: Either[C, D],
      transformer: TransformValue[C, D]): StoredCacheObject[_ <: CacheValue] = value match {
    case Left(value) =>
      new CompressedCacheObject[C, D](key, value, transformer)
    case Right(value) =>
      new DecompressedCacheObject[D, C](key, value, transformer)
  }
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
    _key: Comparable[AnyRef],
    _value: D,
    _transformer: TransformValue[C, D])
    extends StoredCacheObject[D](_key, _value, _transformer) {

  override def isCompressed: Boolean = false

  override def toStats: CacheValueStats = new CacheValueStats(weightageWithoutBoost, generation)

  /**
   * Compress the given object (should be a hard reference to the contained object) and return
   * [[CompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  def compress(value: D): CompressedCacheObject[C, D] = {
    val transformer = this.transformer.asInstanceOf[TransformValue[C, D]]
    new CompressedCacheObject[C, D](key, transformer.compress(value), transformer)
  }
}

/**
 * Implementation of compressed [[StoredCacheObject]]s. Apart from overridden methods,
 * this contains a `decompress` method used by [[EvictionManager]] which uses the
 * [[TransformValue]] that was provided while constructing this object.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
final class CompressedCacheObject[C <: CacheValue, D <: CacheValue](
    _key: Comparable[AnyRef],
    _value: C,
    _transformer: TransformValue[C, D])
    extends StoredCacheObject[C](_key, _value, _transformer) {

  override def isCompressed: Boolean = true

  override def toStats: CacheValueStats =
    new CompressedCacheValueStats(weightageWithoutBoost, generation, compressionSavings)

  /**
   * If the [[CacheValue]] is compressed, then any additional savings due to compression accounted
   * separately in the [[weightage]]. This is used to adjust the [[weightage]] when creating its
   * corresponding decompressed object.
   */
  private[memory] var compressionSavings: Double = _

  /**
   * Decompress the given object (should be a hard reference to the contained object) and return
   * [[DecompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  def decompress(value: C): DecompressedCacheObject[D, C] = {
    val transformer = this.transformer.asInstanceOf[TransformValue[C, D]]
    new DecompressedCacheObject[D, C](key, transformer.decompress(value), transformer)
  }
}
