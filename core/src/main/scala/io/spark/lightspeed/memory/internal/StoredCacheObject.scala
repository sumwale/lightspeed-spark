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

package io.spark.lightspeed.memory.internal

import com.google.common.base.{FinalizableReferenceQueue, FinalizableWeakReference}
import io.spark.lightspeed.memory._

/**
 * Base trait for key+value pairs stored in the in-memory cache by [[EvictionManager]].
 *
 * @tparam T the type of value contained in this object which is cleared on eviction
 */
sealed trait StoredCacheObject[T <: CacheValue] {

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for evictions. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] final var weightage: Double = _

  /**
   * Indicates the number of times a stored object was "resurrected" i.e. evicted and then cached
   * again. The higher this number, the more it will add to [[weightage]] with the expectation
   * that there is a higher likelihood of the object being faulted in again. [[EvictionManager]]
   * implementations can use this field with [[weightage]] and [[key]] to keep some minimal
   * statistics for otherwise evicted objects for some period of time.
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
   * The key for the object which should be the same as that provided in [[EvictionManager]]'s
   * methods like `putObject`, `getDecompressed` etc.
   */
  def key: Comparable[AnyRef]

  /**
   * The [[CacheValue]] instance provided in [[EvictionManager.putObject]] and by loader
   * in [[EvictionManager.getDecompressed]]. This can be `null` if this object has been
   * evicted from cache.
   */
  def value: T

  /**
   * Invoked by [[EvictionManager]] after this object has been evicted from cache after
   * which [[value]] can return null.
   */
  def clearValue(): Unit

  /**
   * The [[TransformValue]] implementation for the [[value]] provided in
   * [[EvictionManager.putObject]] and by loader in [[EvictionManager.getDecompressed]].
   */
  def transformer: TransformValue[_ <: CacheValue, _ <: CacheValue]

  /**
   * Returns true if this object is compressed and false otherwise. If the [[CacheValue]]
   * does not support compression (i.e. [[TransformValue.compressionAlgorithm]] is [[None]]),
   * then this should be false.
   */
  def isCompressed: Boolean
}

object StoredCacheObject {

  lazy val finalizerQueue = new FinalizableReferenceQueue

  /**
   * Create an appropriate [[StoredCacheObject]] for given [[CacheValue]] and [[TransformValue]].
   * This should only be used by [[EvictionManager]] to transform the arguments of
   * [[EvictionManager.putObject]] for storage in the cache.
   */
  def apply[C <: CacheValue, D <: CacheValue](
      key: Comparable[AnyRef],
      value: Either[C, D],
      transformer: TransformValue[C, D]): StoredCacheObject[_ >: C with D] = value match {
    case Left(value) =>
      if (value.needsFinalization) {
        new CompressedCacheObjectWithFinalization[C, D](
          key,
          value,
          transformer,
          value.finalizationInfo)
      } else new CompressedCacheObjectWithoutFinalization[C, D](key, value, transformer)
    case Right(value) =>
      if (value.needsFinalization) {
        new DecompressedCacheObjectWithFinalization[D, C](
          key,
          value,
          transformer,
          value.finalizationInfo)
      } else new DecompressedCacheObjectWithoutFinalization[D, C](key, value, transformer)
  }
}

/**
 * Base trait for implementations of compressed [[StoredCacheObject]]s. Apart from other useful
 * methods, this contains a `decompress` method used by [[EvictionManager]] which uses the
 * [[TransformValue]] that was provided while constructing this object.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
sealed trait CompressedCacheObject[C <: CacheValue, D <: CacheValue]
    extends StoredCacheObject[C] {

  override final def isCompressed: Boolean = true

  private[memory] final var compressionSavings: Double = _

  /**
   * Decompress the given object (should be a hard reference to the contained object) and return
   * [[DecompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  final def decompress(value: C): (DecompressedCacheObject[D, C], D) = {
    val transformer = this.transformer.asInstanceOf[TransformValue[C, D]]
    val result = transformer.decompress(value)
    if (result.needsFinalization) {
      new DecompressedCacheObjectWithFinalization[D, C](
        key,
        result,
        transformer,
        value.finalizationInfo) -> result
    } else {
      new DecompressedCacheObjectWithoutFinalization[D, C](key, result, transformer) -> result
    }
  }
}

/**
 * Base trait for implementations of decompressed [[StoredCacheObject]]s. Apart from other useful
 * methods, this contains a `compress` method used by [[EvictionManager]] which uses the
 * [[TransformValue]] that was provided while constructing this object.
 *
 * @tparam D the type of contained decompressed object
 * @tparam C the type of compressed object obtained after compression
 */
sealed trait DecompressedCacheObject[D <: CacheValue, C <: CacheValue]
    extends StoredCacheObject[D] {

  override final def isCompressed: Boolean = false

  /**
   * Compress the given object (should be a hard reference to the contained object) and return
   * [[CompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  final def compress(value: D): (CompressedCacheObject[C, D], C) = {
    val transformer = this.transformer.asInstanceOf[TransformValue[C, D]]
    val result = transformer.compress(value)
    if (result.needsFinalization) {
      new CompressedCacheObjectWithFinalization[C, D](
        key,
        result,
        transformer,
        result.finalizationInfo) -> result
    } else {
      new CompressedCacheObjectWithoutFinalization[C, D](key, result, transformer) -> result
    }
  }
}

/**
 * Base class for extensions of [[StoredCacheObject]] that do not require any `finalize`.
 * [[EvictionManager]] will simply clear the value contained within after eviction so that
 * any concurrent readers will notice and treat it as a cache miss.
 *
 * @tparam T the type of value contained in this object which is cleared on eviction
 */
sealed abstract class StoredCacheObjectWithoutFinalization[T <: CacheValue](
    override final val key: Comparable[AnyRef],
    private[this] final var _value: T,
    override final val transformer: TransformValue[_ <: CacheValue, _ <: CacheValue])
    extends StoredCacheObject[T] {

  override final def value: T = _value

  override final def clearValue(): Unit = _value = null.asInstanceOf[T]
}

/**
 * Base class for extensions of [[StoredCacheObject]] with `finalizationInfo`. This is a
 * `WeakReference` that will be enqueued by GC in [[StoredCacheObject.finalizerQueue]] once
 * the contained [[CacheValue]] is collected, and the `finalizeReferent` method invoked.
 *
 * @tparam T the type of value contained in this object which is cleared on eviction or when
 *           the [[StoredCacheObject.finalizerQueue]] is processed once GC enqueues this object
 */
sealed abstract class StoredCacheObjectWithFinalization[T <: CacheValue](
    override final val key: Comparable[AnyRef],
    _value: T,
    override final val transformer: TransformValue[_ <: CacheValue, _ <: CacheValue],
    protected final val finalizationInfo: Long)
    extends FinalizableWeakReference[T](_value, StoredCacheObject.finalizerQueue)
    with StoredCacheObject[T] {

  override final def value: T = get()

  override final def clearValue(): Unit = {
    // nothing to be done here; CacheValue.release or StoredCachedObject.finalizerQueue
    // handler thread will clear the referent field
  }

  override def finalizeReferent(): Unit =
    transformer.finalize(key, isCompressed, finalizationInfo)
}

/**
 * Extension of [[CompressedCacheObject]] without any `finalizationInfo`.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
class CompressedCacheObjectWithoutFinalization[C <: CacheValue, D <: CacheValue](
    _key: Comparable[AnyRef],
    _value: C,
    _transformer: TransformValue[C, D])
    extends StoredCacheObjectWithoutFinalization[C](_key, _value, _transformer)
    with CompressedCacheObject[C, D]

/**
 * Extension of [[CompressedCacheObject]] with valid `finalizationInfo`.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
class CompressedCacheObjectWithFinalization[C <: CacheValue, D <: CacheValue](
    _key: Comparable[AnyRef],
    _value: C,
    _transformer: TransformValue[C, D],
    _finalizationInfo: Long)
    extends StoredCacheObjectWithFinalization[C](_key, _value, _transformer, _finalizationInfo)
    with CompressedCacheObject[C, D]

/**
 * Extension of [[DecompressedCacheObject]] without any `finalizationInfo`.
 *
 * @tparam D the type of contained decompressed object
 * @tparam C the type of compressed object obtained after compression
 */
class DecompressedCacheObjectWithoutFinalization[D <: CacheValue, C <: CacheValue](
    _key: Comparable[AnyRef],
    _value: D,
    _transformer: TransformValue[C, D])
    extends StoredCacheObjectWithoutFinalization[D](_key, _value, _transformer)
    with DecompressedCacheObject[D, C]

/**
 * Extension of [[DecompressedCacheObject]] with valid `finalizationInfo`.
 *
 * @tparam D the type of contained decompressed object
 * @tparam C the type of compressed object obtained after compression
 */
class DecompressedCacheObjectWithFinalization[D <: CacheValue, C <: CacheValue](
    _key: Comparable[AnyRef],
    _value: D,
    _transformer: TransformValue[C, D],
    _finalizationInfo: Long)
    extends StoredCacheObjectWithFinalization[D](_key, _value, _transformer, _finalizationInfo)
    with DecompressedCacheObject[D, C]
