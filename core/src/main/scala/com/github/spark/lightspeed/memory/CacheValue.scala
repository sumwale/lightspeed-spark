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
 * An object that can be cached by [[EvictionManager]] and is paired with a unique `key` for the
 * object that is maintained in-memory (either on-heap or off-heap depending on the implementation
 * of this class) by [[EvictionManager]] and can be removed by it when there is memory pressure.
 *
 * The [[EvictionManager]] can deal with two kinds of objects: compressed and decompressed and can
 * transparently switch between compressed and decompressed objects depending on memory pressure
 * and the overall cost (e.g. compression allows for more objects to be cached but there can be
 * significant overhead to decompression, so there needs to be a balance between the two).
 * Implementations of this class should indicate it in the result of `isCompressed` method and
 * provide [[TransformValue]] implementation to transform between the two in the
 * [[EvictionManager]] API methods. If the object has no compressed/decompressed versions, then
 * the `isCompressed` method should return false and [[TransformValue.compressionAlgorithm]]
 * of the passed [[TransformValue]] should be [[None]].
 */
trait CacheValue extends FreeValue {

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  protected[this] final var referenceCount: Int = _

  /**
   * Size in bytes occupied by the object in memory. The size for the `key` should be included
   * in this for accurate bookkeeping by [[EvictionManager]]. Alternatively the memory consumed
   * by keys needs to be accounted separately beyond [[EvictionManager]]'s limit (which is not
   * a memory manager per-se rather a helper for a full-fledged manager that deals with accounting
   * of overall memory like Spark's MemoryManager).
   */
  def memorySize: Long

  /**
   * Whether the object is compressed or not. If there is no compression/decompression desired
   * for the object then this should return false and [[TransformValue.compressionAlgorithm]]
   * should be [[None]] for the passed [[TransformValue]] to [[EvictionManager]] API methods.
   */
  def isCompressed: Boolean

  /**
   * Any [[FinalizeValue]] for this object to be invoked when this object is [[release]]d.
   * If no [[FinalizeValue]] is required for this object then this will always be null.
   */
  private[memory] def finalizer: FinalizeValue[_ <: CacheValue]

  /**
   * Set a [[FinalizeValue]] for this object to be invoked when its [[referenceCount]] goes down
   * to zero in [[release]].
   *
   * @throws IllegalArgumentException if no [[FinalizeValue]] is required for this object which
   *                                  should be indicated by [[TransformValue.createFinalizer]]
   *                                  returning [[None]]
   */
  private[memory] def finalizer_=(finalizer: FinalizeValue[_ <: CacheValue]): Unit

  /**
   * Returns true if the contained object is being used (by [[EvictionManager]] or otherwise)
   * as indicated by [[work]] or [[use]] methods that alter the [[referenceCount]].
   */
  final def isInUse: Boolean = synchronized(referenceCount > 0)

  /**
   * Returns true if the [[referenceCount]] is valid.
   */
  final def isValid: Boolean = synchronized(referenceCount >= 0)

  /**
   * Mark the object as being used to prevent it being `finalized` by incrementing its
   * [[referenceCount]]. Normally callers should ensure that the [[release]] is invoked in
   * some finally block to always decrement the [[referenceCount]]. In the worst case the object
   * will be released by GC using `WeakReference`s. Even though the `WeakReference` approach is
   * much more efficient than `Object.finalize`, it is unpredictable and its preferable for users
   * to explicitly invoke [[release]] especially for off-heap memory because GC will only collect
   * in case of heap pressure (which can be small if off-heap is the primary storage).
   *
   * @throws IllegalStateException if [[referenceCount]] was negative meaning it was already
   *                               `finalized` by [[release]]
   */
  final def use(): Unit = synchronized {
    val ref = referenceCount
    if (ref >= 0) referenceCount = ref + 1
    else {
      throw new IllegalStateException(s"use(): invalid referenceCount=$ref for $toString")
    }
  }

  /**
   * Like [[use]] but will return false instead of throwing [[IllegalStateException]] if the
   * object was already `finalized` by [[release]].
   *
   * @return true if the object was valid and [[referenceCount]] was incremented and false if
   *         the object was already `finalized` by [[release]]
   */
  final def tryUse(): Boolean = synchronized {
    val ref = referenceCount
    if (ref >= 0) {
      referenceCount = ref + 1
      true
    } else false
  }

  /**
   * Decrement the [[referenceCount]] and `finalize` this object if it has gone down to zero
   * (and thus it is already gone from [[EvictionManager]] cache). Any further accesses to this
   * object can result in undefined behaviour if this method returned true. Specifically, accessing
   * off-heap objects after this method returns true can lead to a JVM crash. Used by
   * [[EvictionManager]] when an object is evicted and should be invoked by all other users
   * that are using this object with [[use]] in a finally block.
   *
   * @return true if [[referenceCount]] goes down to zero (in which case the [[referenceCount]] is
   *         set as negative) and object is `finalized` either using the [[finalizer]] if it was
   *         set for the object or else by directly invoking [[free]]
   *
   * @throws IllegalStateException if the [[referenceCount]] is already zero or below meaning
   *                               object was already `finalized` by previous [[release]] calls
   */
  final def release(): Boolean = synchronized {
    val ref = referenceCount
    if (ref > 0) {
      referenceCount = ref - 1
      if (ref == 1) {
        finalizeSelf()
        referenceCount = -1 // indicates that this object has been finalized
        true
      } else false
    } else {
      throw new IllegalStateException(s"release(): invalid referenceCount=$ref for $toString")
    }
  }

  /**
   * Clear the WeakReference, if any, to avoid it being enqueued and [[free]] the object.
   *
   * NOTE: this method is not synchronized by design and callers should either invoke within a
   * synchronized block or ensure that only a single thread can possibly access/update this object.
   */
  private[memory] final def finalizeSelf(): Unit = {
    val f = finalizer
    if (f ne null) {
      f.clear()
      f.finalizeReferent()
    } else free()
  }

  /**
   * All users of this object should work on this object within this method or using
   * [[use]]+[[release]] so that the object is guaranteed to be valid throughout the duration
   * of the method and it ensures that object has not been `finalized` while still in use. If the
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
}

/**
 * Base trait encapsulating the actions required to `finalize` an object like freeing off-heap
 * memory, bookkeeping with Spark's memory manager etc.
 *
 * This trait is implemented both by [[CacheValue]] and [[FinalizeValue]] by design to make it easy
 * for them to inherit a common implementation since those two are required to behave in an
 * identical manner for the same [[CacheValue]] object.
 */
trait FreeValue {

  /**
   * Perform any additional cleanup for the object to `finalize` it (e.g. free memory for off-heap
   * objects) for a [[CacheValue]] which should be identical to that of the [[FinalizeValue]]
   * returned by [[TransformValue.createFinalizer]] (or no-op if that returns [[None]]).
   *
   * NOTE: Any further accesses to the object can result in undefined behaviour once this is
   * invoked. Specifically, accessing off-heap objects after this method can lead to a JVM crash.
   */
  protected def free(): Unit
}

/**
 * Bare statistics of [[CacheValue]]s used by [[EvictionManager]] to record for future either
 * after eviction when the object might be "resurrected", or even persisted for future usage.
 *
 * @param weightage base `weightage` of the cached decompressed object
 * @param generation indicates the number of times an object was evicted and then "resurrected"
 */
final class CacheValueStats(val weightage: Double, val generation: Int) {

  /**
   * Return a new [[CacheValueStats]] or self with added weightage.
   *
   * @param delta extra weightage to be added (can be negative)
   *
   * @return new [[CacheValueStats]] or self with updated weightage
   */
  def addWeightage(delta: Double): CacheValueStats =
    if (delta == 0.0) this else new CacheValueStats(weightage + delta, generation)
}
