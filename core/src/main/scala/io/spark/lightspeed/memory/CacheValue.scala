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

import java.lang.ref.Reference

/**
 * The value object including a unique `key` for the object that is maintained in-memory (either
 * on-heap or off-heap depending on the implementation of this class) by [[EvictionManager]] and
 * can be removed by it when there is memory pressure.
 *
 * The [[EvictionManager]] can deal with two kinds of objects: compressed and decompressed.
 * Implementations can transparently switch between compressed and decompressed objects depending
 * on memory pressure and the overall cost (e.g. compression allows for more objects to be cached
 * but there can be significant overhead to decompression, so there needs to be a balance between
 * the two). Implementations of this class should indicate it in the result of `isCompressed`
 * method and provide [[TransformValue]] implementation to transform between the two in the
 * [[EvictionManager]] API methods. If the object has no compressed/decompressed versions, then
 * the `isCompressed` method should return false and [[TransformValue.compressionAlgorithm]]
 * of the passed [[TransformValue]] should be [[None]].
 */
trait CacheValue {

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  protected[this] final var referenceCount: Int = _

  /**
   * Size in bytes occupied by the object in memory. The size for the `key` should be included
   * in this even if that object is created on-the-fly since [[EvictionManager]] cache will retain
   * a copy of the key. Alternatively the memory consumed by keys needs to be accounted separately
   * beyond [[EvictionManager]]'s limit (which is not a memory manager per-se rather a helper for
   * a full-fledged manager that deals with accounting of overall memory like Spark's UMM).
   */
  def memorySize: Long

  /**
   * Whether the object is compressed or not. If there is no compression/decompression desired
   * for the object then this should return false and [[TransformValue.compressionAlgorithm]]
   * should be [[None]] for the passed [[TransformValue]] to [[EvictionManager]] API methods.
   */
  def isCompressed: Boolean

  /**
   * Minimal information required for finalization by [[TransformValue.finalize]]
   * (typically off-heap address for off-heap objects).
   */
  def finalizationInfo: Long

  /**
   * Returns true if [[finalizationInfo]] is useful and required by [[TransformValue.finalize]].
   * If false, then [[TransformValue.finalize]] and [[free]] methods are skipped for the object.
   */
  def needsFinalization: Boolean

  /**
   * Any [[Reference]] to this object that needs to be cleared when this object is [[free]]'d.
   * If [[needsFinalization]] is false then this will always be null.
   */
  private[memory] def finalizer: Reference[_ <: CacheValue]

  /**
   * Set a [[Reference]] to this object that needs to be cleared when this object is [[free]]'d.
   * This is used by [[EvictionManager]] to help clear `WeakReference` of this object in its cache.
   *
   * @throws IllegalArgumentException if [[needsFinalization]] is false
   */
  private[memory] def finalizer_=(finalizer: Reference[_ <: CacheValue]): Unit

  /**
   * Returns true if the contained object is being used (by [[EvictionManager]] or otherwise)
   * as indicated by [[work]] or [[use]] methods that alter the [[referenceCount]].
   */
  final def isInUse: Boolean = synchronized(referenceCount > 0)

  /**
   * Mark the object as being used to prevent it being [[free]]'d by incrementing its
   * [[referenceCount]]. Normally callers should ensure that the [[release]] is invoked in
   * some finally block to always decrement the [[referenceCount]]. In the worst case the object
   * will be released by GC using `WeakReference`s. Even though the `WeakReference` approach is
   * much more efficient than `Object.finalize`, it is unpredictable and its preferable for users
   * to explicitly invoke [[release]] especially for off-heap memory because GC will only collect
   * in case of heap pressure (which can be small if off-heap is the primary storage).
   *
   * @throws IllegalStateException if the object was already [[free]]'d
   */
  final def use(): Unit = synchronized {
    if (referenceCount >= 0) referenceCount += 1
    else {
      throw new IllegalStateException(
        s"use(): invalid referenceCount=$referenceCount for $toString")
    }
  }

  /**
   * Like [[use]] but will return false instead of throwing [[IllegalStateException]] if the
   * object was already [[free]]'d.
   *
   * @return true if the object was valid and [[referenceCount]] was incremented and false if
   *         the object was already [[free]]'d
   */
  final def tryUse(): Boolean = synchronized {
    if (referenceCount >= 0) {
      referenceCount += 1
      true
    } else false
  }

  /**
   * Decrement the [[referenceCount]] and [[free]] this object if there are no active references
   * (and thus it is already gone from [[EvictionManager]] cache). Any further accesses to this
   * object can result in undefined behaviour if this method returned true. Specifically accessing
   * off-heap objects after this method returned true can lead to a JVM crash. Used by
   * [[EvictionManager]] when an object is evicted and should be invoked by all other users
   * that are using this object with [[use]] in a finally block.
   *
   * @return true if [[referenceCount]] went down to zero and object was [[free]]'d
   *
   * @throws IllegalStateException if the object was already [[free]]'d
   */
  final def release(): Boolean = synchronized {
    if (referenceCount > 0) {
      referenceCount -= 1
      if (referenceCount == 0) {
        if (needsFinalization) {
          val ref = finalizer
          if (ref ne null) {
            ref.clear()
            finalizer = null
          }
          free()
        }
        referenceCount = -1 // indicates that this object has been free()'d
        true
      } else false
    } else {
      throw new IllegalStateException(
        s"release(): invalid referenceCount=$referenceCount for $toString")
    }
  }

  /**
   * All users of this object should work on this object within this method or using
   * [[use]]+[[release]] so that the object is guaranteed to be valid throughout the duration
   * of the method and it ensures that object has not been [[free]]'d while still in use. If the
   * `work` to be done is spread across multiple methods/modules then the [[use]]+[[release]]
   * pair should be used instead.
   */
  final def work[W](f: => W): W = {
    use()
    try {
      f
    } finally {
      release()
    }
  }

  /**
   * Perform any finalization actions to be taken for this cached object. Any further accesses
   * to this object can result in undefined behaviour if this method returned true. Specifically
   * accessing off-heap objects after this method has been invoked can lead to a JVM crash.
   *
   * This should be identical to the [[TransformValue.finalize]] method for the [[TransformValue]]
   * that was passed to [[EvictionManager.putObject]] or by loader in `getDecompressed`.
   */
  protected def free(): Unit
}
