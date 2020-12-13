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
 * the two). This is a sealed trait to ensure that only those two implementations are possible
 * for storage within [[EvictionManager]] while [[PublicCacheObject]] is the implementation
 * exposed to the `outside` world.
 *
 * @tparam T the type of contained object
 */
sealed trait CacheObject[T <: CacheValue] {

  /**
   * A unique key for the object. This can possibly be same as [[value]].
   */
  def key: Comparable[AnyRef]

  /**
   * The value stored in the [[CacheObject]]. For the case of [[StoredCacheObject]] this will
   * return null once the value has been evicted from cache.
   */
  def value: CacheValue

  /**
   * Size in bytes occupied by the object in memory. The size for the key can be included
   * if the object itself doubles as the key, or else the memory consumed by keys need to be
   * accounted for separately (the [[EvictionManager]] is not a memory manager per-se rather
   * is intended as a helper for a full fledged memory manager).
   */
  def memorySize: Long

  /**
   * Whether the object is compressed or not.
   */
  def isCompressed: Boolean

  /**
   * The compression algorithm used by the object, if already compressed,
   * or will be used when it is compressed.
   */
  def compressionAlgorithm: String

  /**
   * Returns true if the contained object is being used (either by EvictionManager or otherwise).
   */
  def isInUse: Boolean

  /**
   * All users of this object should work on this object within this method or using
   * [[startWork]]/[[endWork]] so that the object is guaranteed to be valid throughout the duration
   * of the method and it ensures [[EvictionManager]] will not [[release]] this object. This method
   * should normally be used for some short amount of usage of the object while the
   * [[startWork]]/[[endWork]] pair are intended to be used for longer durations or when usage is
   * spread across multiple methods/modules.
   */
  def work[W](f: => W): W

  /**
   * Mark the object as being used to prevent it being [[release]]d. Normally callers should
   * ensure that the [[endWork]] is invoked in some finally block to always free the object for
   * [[release]]. In the worst case the object will be released by GC when this `WeakReference`
   * is collected. Though the `WeakReference` approach is much more efficient than `finalize`,
   * but it is always preferred for users to explicitly invoke [[endWork]] especially for off-heap
   * memory because GC will only collect in case of heap pressure (which will be minimal if
   * off-heap is the primary storage like Spark configured with off-heap).
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
   * has been invoked can lead to a JVM crash. Normally used by [[EvictionManager]] when an
   * object is evicted but can be used by other users for "unmanaged" objects that failed in put
   * into the [[EvictionManager]]. This will honour [[work]] and [[startWork]] methods on the
   * object and wait for the method to end.
   */
  def release(waitForMillis: Long): Boolean
}

/**
 * The type of value stored in a [[CacheObject]].
 */
trait CacheValue {

  /**
   * Return a key that can be used for long term storage even after this object has been evicted.
   * This can be used by [[EvictionManager]] to keep some stats about previously evicted objects
   * and possibly apply additional policies when those objects are "resurrected".
   *
   * This is required to be different from [[CacheObject.key]] in case if the `key` is same as
   * this value  or contains it otherwise latter may never get evicted from memory since a hard
   * reference to the result of this method can be held for a long time by [[EvictionManager]].
   * Additionally this should be the same as [[CacheObject.key]] as regards the `hashCode`,
   * `equals` and `Comparable.compareTo` methods else it can result in an undefined eviction order.
   */
  def getKeyForLTS(key: Comparable[AnyRef]): Comparable[AnyRef]
}

case class PublicCacheObject[T <: CacheValue, U <: CacheValue](
    override val key: Comparable[AnyRef],
    override val value: T,
    override val memorySize: Long,
    override val isCompressed: Boolean,
    override val compressionAlgorithm: String,
    /**
     * Size in bytes of the other type of object i.e. decompressed if this object is compressed
     * and vice-versa. When this object is a decompressed one, then the compressed size is required
     * to be exact as it will be used by [[EvictionManager]] but if it is a compressed one then
     * the size of decompressed version can be an estimate since that is only used to estimate
     * the overall "savings" due to compression by [[EvictionManager]].
     */
    private[memory] val otherObjectSize: Long,
    /**
     * Transform to the other object type i.e. if `isCompressed` is true, then the decompressed
     * value with size, else compressed value with size.
     */
    private[memory] val toOtherObject: T => (U, Long),
    /**
     * Transform from the other object type i.e. if `isCompressed` is true, then given the
     * decompressed value, return compressed value with size and vice-versa.
     */
    private[memory] val fromOtherObject: U => (T, Long),
    /**
     * Actions to `finalize` the contained object. This should be able to deal with both the
     * compressed and decompressed versions of the object. In normal cases the finalization
     * will deal with release of off-heap memory which should be identical for
     * compressed/decompressed objects.
     */
    private[memory] val doFinalize: CacheObject[_ <: CacheValue] => Unit,
    /**
     * Used internally by [[EvictionManager]] to store a reference to the internal [[CacheObject]]
     * which is used for `word`, `startWork`, `endWork` and `release` methods.
     */
    private[memory] val stored: Option[StoredCacheObject[_ <: CacheValue]] = None)
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
  private[memory] def toStoredObject: StoredCacheObject[T] = stored match {
    case Some(o) => o.asInstanceOf[StoredCacheObject[T]]
    case _ =>
      if (isCompressed) {
        new CompressedCacheObject[T, U](
          key,
          value,
          memorySize,
          compressionAlgorithm,
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

private[memory] sealed abstract class StoredCacheObject[T <: CacheValue](
    _key: Comparable[AnyRef],
    _value: T,
    override final val memorySize: Long,
    override final val compressionAlgorithm: String,
    private[memory] final val doFinalize: CacheObject[_ <: CacheValue] => Unit)
    extends FinalizableWeakReference[T](_value, StoredCacheObject.finalizerQueue)
    with CacheObject[T] {

  /**
   * The current key for the stored object which can change when switching to LTS
   * (long term storage).
   */
  private[this] final var storedKey: Comparable[AnyRef] = _key

  /**
   * Used internally and should only be accessed/updated within synchronized block.
   */
  @volatile private[this] final var inUse: Int = _

  override final def key: Comparable[AnyRef] = storedKey

  /**
   * Change the stored key. Should only be used by [[EvictionManager]] during eviction to change
   * to [[CacheValue.getKeyForLTS]] or vice-versa when a removed object is "resurrected".
   */
  private[memory] final def setKey(key: Comparable[AnyRef]): Unit = synchronized {
    storedKey = key
  }

  override final def value: T = get()

  /**
   * Size in bytes of the compressed version of this stored object.
   */
  def compressedSize: Long

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

  override final def startWork(): Unit = synchronized {
    inUse += 1
  }

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
      clear()
      finalizeReferent()
      true
    } else false
  }

  override def finalizeReferent(): Unit = if (doFinalize ne null) doFinalize(this)

  private[memory] final def reset(): Unit = synchronized {
    inUse = 0
    weightage = 0.0
    // nextInQueue = null
    // previousInQueue = null
  }

  /**
   * The overall `weightage` determined for this object used by [[EvictionManager]] to order the
   * objects for evictions. A lower `weightage` implies a higher chance of eviction and vice-versa.
   */
  private[memory] final var weightage: Double = _

  /**
   * Previous object in the queue maintained by [[EvictionManager]].
   */
  // private[memory] final var previousInQueue: CacheObject[T] = _

  /**
   * Next object in the queue maintained by [[EvictionManager]].
   */
  // private[memory] final var nextInQueue: CacheObject[T] = _
}

object StoredCacheObject {
  private[memory] lazy val finalizerQueue = new FinalizableReferenceQueue
}

/**
 * Implementation of compressed object for [[CacheObject]]. Apart from other useful methods,
 * this contains a `decompress` method used by [[EvictionManager]] which uses a closure provided
 * in the constructor of this class.
 *
 * @tparam C the type of contained compressed object
 * @tparam D the type of decompressed object obtained after decompression
 */
private[memory] final class CompressedCacheObject[C <: CacheValue, D <: CacheValue](
    _key: Comparable[AnyRef],
    _value: C,
    _memorySize: Long,
    _compressionAlgorithm: String,
    private[memory] val decompressObject: C => (D, Long),
    private[memory] val compressObject: D => (C, Long),
    _doFinalize: CacheObject[_ <: CacheValue] => Unit)
    extends StoredCacheObject[C](_key, _value, _memorySize, _compressionAlgorithm, _doFinalize) {

  override def isCompressed: Boolean = true

  override def compressedSize: Long = memorySize

  private[memory] var compressionSavings: Double = _

  /**
   * Decompress the given object (usually the contained object itself) and return
   * [[DecompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
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
private[memory] final class DecompressedCacheObject[D <: CacheValue, C <: CacheValue](
    _key: Comparable[AnyRef],
    _value: D,
    _memorySize: Long,
    _compressionAlgorithm: String,
    override val compressedSize: Long,
    private[memory] val compressObject: D => (C, Long),
    private[memory] val decompressObject: C => (D, Long),
    _doFinalize: CacheObject[_ <: CacheValue] => Unit)
    extends StoredCacheObject[D](_key, _value, _memorySize, _compressionAlgorithm, _doFinalize) {

  override def isCompressed: Boolean = false

  /**
   * Compress the given object (usually the contained object itself) and return
   * [[CompressedCacheObject]] and contained object. Should only be used by [[EvictionManager]].
   */
  private[memory] def compress(value: D): (CompressedCacheObject[C, D], C) = {
    val (compressedValue, compressedSize) = compressObject(value)
    new CompressedCacheObject[C, D](
      key,
      compressedValue,
      compressedSize,
      compressionAlgorithm,
      decompressObject,
      compressObject,
      doFinalize) -> compressedValue
  }
}
