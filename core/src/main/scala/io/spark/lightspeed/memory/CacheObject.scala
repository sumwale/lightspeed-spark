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

import com.google.common.base.FinalizableWeakReference

/**
 * An object that is maintained in-memory (either on-heap or off-heap depending on the actual
 * object being cached) by [[EvictionManager]] and can be removed when there is memory pressure.
 *
 * The [[EvictionManager]] deals with two kinds of objects: compressed and decompressed.
 * Implementations can transparently switch between compressed and decompressed objects depending
 * on memory pressure and the overall cost (e.g. compression allows for more objects to be cached
 * but there can be significant overhead to decompression, so there needs to be a balance between
 * the two). This is a sealed trait to ensure that only those two implementations are possible.
 *
 * @tparam T the type of contained object
 */
sealed trait CacheObject[T] extends FinalizableWeakReference[T] {

  /**
   * A unique key for the object. This can possibly be this object itself.
   */
  def key: AnyRef

  /**
   * Size in bytes occupied by the object in memory including the key.
   */
  def memorySize: Long

  /**
   * Whether the object is compressed or not.
   */
  def isCompressed: Boolean

  /**
   * The compression algorithm used by the object, if already compressed,
   * or will be used when it is compressed if not.
   */
  def compressionAlgorithm: String

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  protected[this] var inUse: Int = _

  /**
   * All users of this object should do so within this method or [[startWork]]/[[endWork]] so that
   * the object is guaranteed to be valid throughout the duration of the method and
   * [[EvictionManager]] will not [[release]] this object. This method should normally be used
   * for some short amount of usage of the object while the [[startWork]]/[[endWork]] pair should
   * be used for longer durations or when usage is spread across multiple methods/modules.
   */
  final def work[U](f: => U): U = synchronized {
    inUse += 1
    try {
      f
    } finally {
      inUse -= 1
      notifyAll()
    }
  }

  /**
   * Mark the object as being used to prevent it being [[release]]d. Normally callers should
   * ensure that the [[endWork]] is invoked in some finally block to always free the object for
   * [[release]]. In the worst case the object will be released by GC when this `WeakReference`
   * is collected. Though this is much more efficient than `finalize`, but it is always preferred
   * for users to explicitly invoke [[endWork]] especially for off-heap memory because GC will
   * only collect in case of heap pressure.
   */
  final def startWork(): Unit = synchronized(inUse += 1)

  /**
   * Un-mark the object as being in-use. This is a counter and should always follow [[startWork]].
   */
  final def endWork(): Unit = synchronized {
    inUse -= 1
    notifyAll()
  }

  /**
   * Release this object from memory (and thus from the EvictionManager).
   * Any further accesses to this object can result in undefined behaviour after
   * this method call. Specifically accessing off-heap objects after this method
   * has been invoked can lead to a JVM crash. Normally used by
   * [[EvictionManager.runEviction]] when an object is evicted. This will honour
   * [[work]] method on the object and wait for the method to end.
   */
  private[memory] final def release(waitForMillis: Long): Unit = synchronized {
    if (inUse > 0) {
      val loopMillis = if (waitForMillis > 10) 10 else waitForMillis
      var remaining = waitForMillis
      while (remaining > 0) {
        try {
          wait(loopMillis)
          if (inUse == 0) remaining = 0 else remaining -= loopMillis
        } catch {
          case _: InterruptedException =>
            remaining = 0
            Thread.currentThread().interrupt()
        }
      }
    }
    if (inUse == 0) finalizeReferent()
  }

  /**
   * Actions to `finalize` the contained object. This should be able to deal with both the
   * compressed and decompressed versions of the object. In normal cases the finalization will deal
   * with release of off-heap memory which should be identical for compressed/decompressed objects.
   */
  protected val doFinalize: Any => Unit

  override def finalizeReferent(): Unit = doFinalize(get())

  /**
   * Get/Set the cost to transform from decompressed form to compressed and vice-versa.
   * This should normally be filled in by the [[EvictionManager]] as an estimate going
   * by past operations and can be a factor to determine whether the object should
   * be retained or evicted from memory.
   */
  // private[memory] var transformationCost: Double = _

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for evictions. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] var weightage: Double = _

  /**
   * Previous object in the queue maintained by [[EvictionManager]].
   */
  private[memory] var previousInQueue: CacheObject[T] = _

  /**
   * Next object in the queue maintained by [[EvictionManager]].
   */
  private[memory] var nextInQueue: CacheObject[T] = _
}

/**
 * Implementation of compressed object for [[CacheObject]]. Apart from other useful methods,
 * this contains a `decompress` method used by [[EvictionManager]] which uses a closure provided
 * in the constructor of this class.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
final class CompressedCacheObject[C, D](
    override val key: AnyRef,
    _value: C,
    override val memorySize: Long,
    override val compressionAlgorithm: String,
    val decompressedSizeEstimate: Long,
    private val compressObject: D => C,
    private val decompressObject: C => (D, Long),
    override protected val doFinalize: Any => Unit)
    extends FinalizableWeakReference[C](_value, EvictionManager.finalizerQueue)
    with CacheObject[C] {

  override def isCompressed: Boolean = true

  /**
   * Decompress the contained object and return a [[DecompressedCacheObject]]. Normally should
   * be used by [[EvictionManager]].
   */
  private[memory] def decompress(): DecompressedCacheObject[D, C] = {
    val (decompressedValue, decompressedSize) = decompressObject(get())
    new DecompressedCacheObject[D, C](
      key,
      decompressedValue,
      decompressedSize,
      compressionAlgorithm,
      memorySize,
      compressObject,
      decompressObject,
      doFinalize)
  }
}

/**
 * Implementation of decompressed object for [[CacheObject]]. Apart from other useful methods,
 * this contains a `compress` method used by [[EvictionManager]] which uses a closure provided
 * in the constructor of this class. Note that this object is created transparently by the
 * [[EvictionManager]] while compressed version has to be created explicitly by callers.
 *
 * @tparam D the type of contained decompressed object
 * @tparam C the type of compressed object obtained after compression
 */
final class DecompressedCacheObject[D, C](
    override val key: AnyRef,
    _value: D,
    override val memorySize: Long,
    override val compressionAlgorithm: String,
    val compressedSize: Long,
    private val compressObject: D => C,
    private val decompressObject: C => (D, Long),
    override protected val doFinalize: Any => Unit)
    extends FinalizableWeakReference[D](_value, EvictionManager.finalizerQueue)
    with CacheObject[D] {

  override def isCompressed: Boolean = false

  /**
   * Compress the contained object and return a [[CompressedCacheObject]]. Normally should
   * be used by [[EvictionManager]].
   */
  private[memory] def compress(): CompressedCacheObject[C, D] = {
    val compressedValue = compressObject(get())
    new CompressedCacheObject[C, D](
      key,
      compressedValue,
      compressedSize,
      compressionAlgorithm,
      memorySize,
      compressObject,
      decompressObject,
      doFinalize)
  }
}
