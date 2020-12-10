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

import com.google.common.base.{FinalizableReferenceQueue, FinalizableWeakReference}

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
sealed trait CacheObject[T] {

  /**
   * A unique key for the object. This can possibly be this object itself.
   */
  def key: Comparable[AnyRef]

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
   * Returns true if the contained object is being used (either by EvictionManager or otherwise).
   */
  def isInUse: Boolean

  /**
   * All users of this object should do so within this method or [[startWork]]/[[endWork]] so that
   * the object is guaranteed to be valid throughout the duration of the method and
   * [[EvictionManager]] will not [[release]] this object. This method should normally be used
   * for some short amount of usage of the object while the [[startWork]]/[[endWork]] pair should
   * be used for longer durations or when usage is spread across multiple methods/modules.
   */
  def work[W](f: => W): W

  /**
   * Mark the object as being used to prevent it being [[release]]d. Normally callers should
   * ensure that the [[endWork]] is invoked in some finally block to always free the object for
   * [[release]]. In the worst case the object will be released by GC when this `WeakReference`
   * is collected. Though this is much more efficient than `finalize`, but it is always preferred
   * for users to explicitly invoke [[endWork]] especially for off-heap memory because GC will
   * only collect in case of heap pressure.
   */
  def startWork(): Unit

  /**
   * Un-mark the object as being in-use. This is a counter and should always follow [[startWork]].
   */
  def endWork(): Unit

  /**
   * Release this object from memory (and thus from the [[EvictionManager]]).
   * Any further accesses to this object can result in undefined behaviour if
   * this method returned true. Specifically accessing off-heap objects after this method
   * has been invoked can lead to a JVM crash. Normally used by
   * [[EvictionManager]] when an object is evicted but can be used by other users for "unmanaged"
   * objects that failed in put into the [[EvictionManager]]. This will honour [[work]] and
   * [[startWork]] methods on the object and wait for the method to end.
   */
  def release(waitForMillis: Long): Boolean
}

object CacheObject {
  private[memory] lazy val finalizerQueue = new FinalizableReferenceQueue
}

final class PublicCacheObject[T, U](
    override val key: Comparable[AnyRef],
    val value: T,
    override val memorySize: Long,
    override val isCompressed: Boolean,
    override val compressionAlgorithm: String,
    private[memory] val otherObjectSize: Long,
    private[memory] val toOtherObject: T => (U, Long),
    private[memory] val fromOtherObject: U => (T, Long),
    private[memory] val doFinalize: CacheObject[_] => Unit,
    private[memory] val stored: Option[StoredCacheObject[_, _]] = None)
    extends CacheObject[T] {

  override def isInUse: Boolean = stored match {
    case Some(o) => o.isInUse
    case _ => false
  }

  override def work[W](f: => W): W = stored match {
    case Some(o) => o.work(f)
    case _ => f
  }

  override def startWork(): Unit = stored match {
    case Some(o) => o.startWork()
    case _ =>
  }

  override def endWork(): Unit = stored match {
    case Some(o) => o.endWork()
    case _ =>
  }

  override def release(waitForMillis: Long): Boolean = stored match {
    case Some(o) => o.release(waitForMillis)
    case _ => true
  }

  /**
   * Convert this [[CacheObject]] in a form suitable for storage i.e. one of
   * [[CompressedCacheObject]] or [[DecompressedCacheObject]]. This should only be used
   * by [[EvictionManager]] to transform the [[EvictionManager.putObject]] to storage form.
   */
  private[memory] def toStoredObject: StoredCacheObject[T, U] = stored match {
    case Some(o) => o.asInstanceOf[StoredCacheObject[T, U]]
    case _ =>
      if (isCompressed) {
        new CompressedCacheObject[T, U](
          key,
          value,
          memorySize,
          compressionAlgorithm,
          otherObjectSize,
          toOtherObject,
          fromOtherObject,
          doFinalize)
      } else {
        new DecompressedCacheObject[T, U](
          key,
          value,
          memorySize,
          compressionAlgorithm,
          otherObjectSize,
          toOtherObject,
          fromOtherObject,
          doFinalize)
      }
  }
}

private[memory] abstract class StoredCacheObject[T, U](
    override val key: Comparable[AnyRef],
    _value: T,
    override val memorySize: Long,
    override val compressionAlgorithm: String,
    /**
     * Actions to `finalize` the contained object. This should be able to deal with both the
     * compressed and decompressed versions of the object. In normal cases the finalization
     * will deal with release of off-heap memory which should be identical for
     * compressed/decompressed objects.
     */
    private[memory] val doFinalize: CacheObject[_] => Unit)
    extends FinalizableWeakReference[T](_value, CacheObject.finalizerQueue)
    with CacheObject[T] {

  /**
   * Size in bytes of the compressed version of this stored object.
   */
  def compressedSize: Long

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  @volatile private[this] var inUse: Int = _

  override final def isInUse: Boolean = synchronized(inUse != 0)

  override final def work[W](f: => W): W = synchronized {
    inUse += 1
    try {
      f
    } finally {
      inUse -= 1
      notifyAll()
    }
  }

  override final def startWork(): Unit = synchronized(inUse += 1)

  override final def endWork(): Unit = synchronized {
    inUse -= 1
    notifyAll()
  }

  override final def release(waitForMillis: Long): Boolean = synchronized {
    if (inUse > 0) {
      val loopMillis = if (waitForMillis > 10L) 10L else waitForMillis
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
    if (inUse == 0) {
      finalizeReferent()
      true
    } else false
  }

  override def finalizeReferent(): Unit = if (doFinalize ne null) doFinalize(this)

  private[memory] def reset(): Unit = synchronized {
    inUse = 0
    weightage = 0.0
    // nextInQueue = null
    // previousInQueue = null
  }

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for evictions. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] var weightage: Double = _

  /**
   * Previous object in the queue maintained by [[EvictionManager]].
   */
  // private[memory] var previousInQueue: CacheObject[T] = _

  /**
   * Next object in the queue maintained by [[EvictionManager]].
   */
  // private[memory] var nextInQueue: CacheObject[T] = _
}

/**
 * Implementation of compressed object for [[CacheObject]]. Apart from other useful methods,
 * this contains a `decompress` method used by [[EvictionManager]] which uses a closure provided
 * in the constructor of this class.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
private[memory] final class CompressedCacheObject[C, D](
    _key: Comparable[AnyRef],
    _value: C,
    _memorySize: Long,
    _compressionAlgorithm: String,
    val decompressedSizeEstimate: Long,
    private[memory] val decompressObject: C => (D, Long),
    private[memory] val compressObject: D => (C, Long),
    _doFinalize: CacheObject[_] => Unit)
    extends StoredCacheObject[C, D](_key, _value, _memorySize, _compressionAlgorithm, _doFinalize) {

  override def isCompressed: Boolean = true

  override def compressedSize: Long = memorySize

  /**
   * Decompress the given object (usually the contained object itself) and return a
   * [[DecompressedCacheObject]]. Should only be used by [[EvictionManager]].
   */
  private[memory] def decompress(value: C): (DecompressedCacheObject[D, C], D) = {
    val (decompressedValue, decompressedSize) = decompressObject(value)
    new DecompressedCacheObject[D, C](
      key,
      decompressedValue,
      decompressedSize,
      compressionAlgorithm,
      memorySize,
      compressObject,
      decompressObject,
      doFinalize) -> decompressedValue
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
private[memory] final class DecompressedCacheObject[D, C](
    _key: Comparable[AnyRef],
    _value: D,
    _memorySize: Long,
    _compressionAlgorithm: String,
    override val compressedSize: Long,
    private[memory] val compressObject: D => (C, Long),
    private[memory] val decompressObject: C => (D, Long),
    _doFinalize: CacheObject[_] => Unit)
    extends StoredCacheObject[D, C](_key, _value, _memorySize, _compressionAlgorithm, _doFinalize) {

  override def isCompressed: Boolean = false

  /**
   * Compress the given object (usually the contained object itself) and return a
   * [[CompressedCacheObject]]. Should only be used by [[EvictionManager]].
   */
  private[memory] def compress(value: D): (CompressedCacheObject[C, D], C) = {
    val (compressedValue, compressedSize) = compressObject(value)
    new CompressedCacheObject[C, D](
      key,
      compressedValue,
      compressedSize,
      compressionAlgorithm,
      memorySize,
      decompressObject,
      compressObject,
      doFinalize) -> compressedValue
  }
}
