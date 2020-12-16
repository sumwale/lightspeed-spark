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
   * Size in bytes occupied by the object in memory. The size for the key can be excluded if the
   * object contains reference to the same key, or else the memory consumed by keys needs to be
   * included or accounted separately ([[EvictionManager]] is not a memory manager per-se rather
   * is intended as a helper for a manager that deals with accounting of heap/off-heap memory).
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
   */
  def needsFinalization: Boolean

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  @volatile private[this] final var referenceCount: Int = _

  /**
   * Returns true if the contained object is being used (by [[EvictionManager]] or otherwise)
   * as indicated by [[work]] or [[startWork]] methods that alter the [[referenceCount]].
   */
  final def isInUse: Boolean = synchronized(referenceCount != 0)

  /**
   * All users of this object should work on this object within this method or using
   * [[startWork]]+[[endWork]] so that the object is guaranteed to be valid throughout the duration
   * of the method and it ensures [[EvictionManager]] will not [[release]] this object. This method
   * should normally be used for some short amount of usage of the object while the
   * [[startWork]]+[[endWork]] pair are intended to be used for longer durations or when usage is
   * spread across multiple methods/modules.
   */
  final def work[W](f: => W): W = synchronized {
    referenceCount += 1
    try {
      f
    } finally {
      referenceCount -= 1
      notifyAll()
    }
  }

  /**
   * Mark the object as being used to prevent it being [[release]]d. Normally callers should
   * ensure that the [[endWork]] is invoked in some finally block to always free the object for
   * [[release]]. In the worst case the object will be released by GC using `WeakReference`s.
   * Even though the `WeakReference` approach is much more efficient than `Object.finalize`,
   * it is unpredictable and its preferable for users to explicitly invoke [[endWork]] especially
   * for off-heap memory because GC will only collect in case of heap pressure (which can be
   * small if off-heap is the primary storage like Spark configured with off-heap).
   */
  final def startWork(): Unit = synchronized {
    referenceCount += 1
  }

  /**
   * Un-mark the object as being in-use. This should always be paired with [[startWork]] in a
   * finally block else the [[EvictionManager]] will not be able to [[release]] the object even
   * after being evicted (and will likely only be handled much later by GC).
   */
  final def endWork(): Unit = synchronized {
    referenceCount -= 1
    notifyAll()
  }

  /**
   * Release this object from memory (and thus from the [[EvictionManager]] cache). Any further
   * accesses to this object can result in undefined behaviour if this method returned true.
   * Specifically accessing off-heap objects after this method has been invoked can lead to a JVM
   * crash. Normally used by [[EvictionManager]] when an object is evicted but can be used by other
   * users for "unmanaged" objects that failed in put into the [[EvictionManager]]. This will
   * honor [[work]] and [[startWork]] methods on the object and wait for reference counters
   * incremented by those methods to go down to zero.
   */
  final def release(transformer: TransformValue[_, _], waitMillis: Long): Boolean = synchronized {
    if (referenceCount > 0) {
      val loopMillis = if (waitMillis > 10L) 10L else waitMillis
      var remaining = waitMillis
      while (remaining > 0) {
        try {
          wait(loopMillis)
          if (referenceCount == 0) remaining = 0 else remaining -= loopMillis
        } catch {
          case _: InterruptedException =>
            remaining = 0
            Thread.currentThread().interrupt()
        }
      }
    }
    if (referenceCount == 0) {
      transformer.finalize(isCompressed, finalizationInfo)
      true
    } else false
  }
}
