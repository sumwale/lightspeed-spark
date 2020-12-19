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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import javax.annotation.concurrent.GuardedBy

import io.spark.lightspeed.memory._

import org.apache.spark.internal.Logging

/**
 * An adaptive [[EvictionManager]] that uses a combination of LRU and MFU where aging leads to
 * higher chance of an object being evicted regardless of how frequently it was used in the past.
 * Specifically the implementation maintains a `weightage` for each [[StoredCacheObject]] that
 * is updated using current timestamp on access. Each access will add to the `weightage`, whereas
 * more recent access will also have a higher timestamp. To maintain a balance between the two
 * contributing factors, an exponent is applied to the current timestamp before adding to the
 * weightage where the exponent is calculated based on the `millisForTwiceFreq` parameter that
 * specifies the milliseconds after which the timestamp will have same weightage as two
 * accesses as of now.
 *
 * Switching between caching of compressed and decompressed versions of the object is done
 * based on the following policies:
 *
 * 1) Getter will always cache the decompressed version, if not present. So decompressed objects
 *    will keep filling memory till memory limit is not reached.
 * 2) Once the limit is reached, eviction will kick in to check if compressing the `oldest`
 *    decompressed object is more efficient than evicting it (see more on this in the notes later).
 * 3) In most cases point 2 will lead to the decision of compression but if `weightage` of
 *    `oldest` compressed object is too low compared to `oldest` decompressed object, then
 *    choose to evict the `oldest` compressed object instead.
 * 4) If eviction hits on a compressed object to evict rather than a decompressed one, then
 *    always evict it as usual. The `weightage` of compressed objects is already increased
 *    as per the savings due to compression (see more on this in the notes later).
 *
 * Below are other implementation notes and general thoughts.
 *
 * If block is not evicted rather compressed then:
 * - cost to compress once (can be ignored), then decompress on every read
 * - the memory space occupied by compressed block could be used by a decompressed block (partial)
 *   or higher priority compressed block
 *
 * If evicted rather than compressed then:
 * - cost to read compressed block from disk on every read
 *
 * In the end when memory runs out, then maintaining a mix of high priority decompressed blocks
 * plus remaining compressed blocks seems to be the best bet. How to determine this balance i.e.
 * cut-off point for the 'priority' or `weightage` as the implementation is calculating.
 * The above factors should determine that but in all cases it can be assumed that caching a
 * compressed block in memory is definitely cheaper than reading from disk everytime.
 *
 * For other cases extra factors in `weightage` for:
 *
 * a) cost of decompressing a compressed block will lower `weightage`
 *
 * b) higher degree of compression will increase `weightage`. Of course, recent usage will
 *    lead to increase in `weightage` as it does now but will also take into account these two
 *    factors for compressed blocks.
 *
 * For a), the cost of decompressing (i.e. millis required in wall clock time for a given
 * decompressed output size) can be noted for a compression algorithm the first time and
 * refreshed at some time interval (e.g. 1 hour). So then b) can be adjusted in the total
 * `weightage` using the scaled wall clock time. For example say compression reduced the block
 * size to half. So this means two compressed blocks can be stored in the same space as one
 * decompressed block. Which means that this saves reading from disk of one compressed block.
 * The difference between a) and b) will then be adjusted as a factor in the total `weightage`.
 *
 * For example if saving for reading a compressed block in b) is 10ms, then one can take that as
 * equivalent to one extra recent access (configurable) and a) is 20ms, then (a - b) is 10ms which
 * is equivalent weightage of one extra recent access. So total weightage of this block should
 * return this one added in (or subtracted if b > a).
 *
 * To simplify things, the final implementation does not actually determine the time to read
 * blocks from disk (which can vary wildly depending on machine/disk state), so just some
 * factor adjustment is made to accommodate the relative time for decompression. If compression
 * reduces size by half, then it is assumed that time to read a compressed block from disk
 * is significantly larger than the time to decompress, so the saving is reduced by a factor
 * which is 0.8 by default but can be configured if really required.
 *
 * @param maxMemorySize the initial maximum memory limit
 * @param millisForTwiceFreq milliseconds after which the timestamp will have same weightage
 *                           as two accesses as of now
 * @param decompressionToDiskReadCost ratio of cost to decompress a disk block to reading it
 *                                    from disk (without OS caching)
 * @param maxMemoryOverflowBeforeEvict maximum ratio of `maxMemorySize` that can overflow before
 *                                     eviction would forcefully block putter/getter threads
 * @param maxEvictedEntries maximum count of old evicted entries before all their stats are purged
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
final class AdaptiveEvictionManager[C <: CacheValue, D <: CacheValue](
    maxMemorySize: Long,
    millisForTwiceFreq: Long,
    decompressionToDiskReadCost: Double,
    maxMemoryOverflowBeforeEvict: Double,
    maxEvictedEntries: Int)
    extends EvictionManager[C, D]
    with Logging {

  def this(maxMemorySize: Long, millisForTwiceFreq: Long) = {
    this(
      maxMemorySize,
      millisForTwiceFreq,
      decompressionToDiskReadCost = 0.2,
      maxMemoryOverflowBeforeEvict = 0.05,
      maxEvictedEntries = 1024 * 1024)
  }

  require(maxMemorySize > 0L, s"maximum memory size is $maxMemorySize")
  require(
    millisForTwiceFreq > 0L,
    s"millis parameter to adjust weightage of timestamp is $millisForTwiceFreq")
  require(
    decompressionToDiskReadCost > 0.0 && decompressionToDiskReadCost < 1.0,
    "ratio of cost to decompress a disk block to reading from disk should be (0.0, 1.0) " +
      s"but was $decompressionToDiskReadCost")
  require(
    maxMemoryOverflowBeforeEvict > 0.0 && maxMemoryOverflowBeforeEvict < 1.0,
    "maximum ratio that can overflow before blocking for eviction should be (0.0, 1.0) " +
      s"but was $maxMemoryOverflowBeforeEvict")
  require(maxEvictedEntries > 0, s"maximum count of evicted entries is $maxEvictedEntries")

  /**
   * The current value of the upper limit on memory in bytes allowed.
   * Should always be accessed within [[maxMemoryLock]].
   */
  @GuardedBy("maxMemoryLock")
  private[this] var maxMemory: Long = maxMemorySize

  /**
   * Lock used to access/update [[maxMemory]]. The [[setLimit]] operation acquires the write lock
   * while all other operations that intend to read [[maxMemory]] and depend on checking the limit
   * should acquire the read lock for the duration of the operation.
   */
  private[this] val maxMemoryLock = new ReentrantReadWriteLock()

  /**
   * This should accurately track the total size of items in [[cacheMap]] as well as
   * [[pendingWorkItems]].
   */
  private[this] val usedMemory = new AtomicLong(0L)

  /**
   * Lock acquired by [[runEviction]] for synchronization.
   * Used instead of a synchronized block to enable use of tryLock().
   */
  private[this] val evictionLock = new ReentrantLock()

  /**
   * The main map that keeps the cached [[StoredCacheObject]] for the given key. Reads will
   * always consult this map to return the cached value else invoke loader if absent.
   */
  private[this] val cacheMap =
    new ConcurrentHashMap[Comparable[AnyRef], StoredCacheObject[_ <: CacheValue]]()

  /**
   * The map used for ordering the cached [[StoredCacheObject]]s by their `weightage` and uses
   * that order to evict those with smallest weightage when [[usedMemory]] exceeds [[maxMemory]].
   *
   * This should always be consistent with [[cacheMap]] in that the [[StoredCacheObject]] in the
   * [[cacheMap]] against a key should be the key in [[evictionMap]]. If this invariant fails then
   * the cache can end up having two [[CacheValue]]s stored against the same key and/or two
   * [[StoredCacheObject]]s for the same key and lead to a lot of trouble. This invariant is
   * ensured by having only a single thread update the two maps during [[runEviction]] under the
   * [[evictionLock]] and is asserted by the implementations of [[PendingEntry]].
   *
   * NOTE: need to add a "strong" reference to value somewhere else value can get GCed at any
   * point, hence the [[CacheValue]] is stored as value in the evictionMap.
   */
  private[this] val evictionMap =
    new ConcurrentSkipListMap[StoredCacheObject[_ <: CacheValue], CacheValue](
      (o1: StoredCacheObject[_ <: CacheValue], o2: StoredCacheObject[_ <: CacheValue]) => {
        if (o1 ne o2) {
          val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
          if (cmp != 0) cmp
          else {
            val c = o1.key.compareTo(o2.key.asInstanceOf[AnyRef])
            // order compressed having same key to be lower than decompressed
            if (c != 0) c else java.lang.Boolean.compare(!o1.isCompressed, !o2.isCompressed)
          }
        } else 0
      })

  /**
   * These are the entries thrown out of [[cacheMap]] and [[evictionMap]] by eviction but
   * are retained for:
   *
   * 1) [[StoredCacheObjectWithFinalization]] objects as `WeakReference`s are required to
   *    properly dispose off [[CacheValue]]s that have `needsFinalization` flag as true.
   *
   * 2) When evicted entries are "resurrected" by a future [[getDecompressed]] operation,
   *    then the retained weightage is used for [[StoredCacheObject.generation]] handling.
   *
   * This map is only cleared when the count of entries exceeds the provided [[maxEvictedEntries]]
   * (the `LinkedHashMap` is used for the same purpose to purge oldest inserted entries first).
   *
   * NOTE: This map is supposed to be accessed/updated only by the eviction thread hence is
   * not concurrent.
   */
  @GuardedBy("evictionLock")
  private[this] val evictedEntries =
    new java.util.LinkedHashMap[Comparable[AnyRef], StoredCacheObject[_ <: CacheValue]]()

  /**
   * Map to hold pending caching work. Callers should ensure that [[usedMemory]] should not exceed
   * [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] before pushing new work items
   * into this map else thread should either fail the caching or block on [[runEviction]].
   */
  private[this] val pendingWorkItems =
    new ConcurrentHashMap[Comparable[AnyRef], PendingEntry[_ <: CacheValue]]()

  /**
   * A read-write lock for [[pendingWorkItems]]. Threads that are reading or pushing new work
   * items should acquire the read lock so that all of them can proceed concurrently, while
   * the eviction thread can acquire the write lock when draining the map. This is required
   * so that the eviction thread can finish its work and not keep iterating possibly
   * indefinitely as new work items can keep being pushed continuously.
   */
  private[this] val pendingWorkLock = new ReentrantReadWriteLock()

  /**
   * Last timestamp at which [[currentTimeAdjustmentExp]] was calculated. This doesn't need to be
   * absolutely current, so is neither volatile nor any lock used while updating this.
   */
  private[this] var lastCurrentTimeAdjustment: Long = System.currentTimeMillis()

  /**
   * This is the exponent used to blow up current time contribution to `weightage` so that those
   * having higher priority over past accesses beyond the small increase in milliseconds.
   * The way this is evaluated is to use the passed [[millisForTwiceFreq]] to determine the time
   * after which two accesses in the past are equivalent to one current access. So this is
   * the `X` in the equation: `currentTime ^ X = 2 * (currentTime - millisForTwiceFreq)`
   */
  private[this] var currentTimeAdjustmentExp: Double =
    calcCurrentTimeAdjustment(lastCurrentTimeAdjustment)

  private def calcCurrentTimeAdjustment(currentTime: Long): Double = {
    val twiceFreqTime = currentTime - millisForTwiceFreq
    math.log(timeToDouble(twiceFreqTime) * 2.0) / math.log(timeToDouble(currentTime))
  }

  /**
   * Convert timestamp longs to a reasonable double value.
   */
  private def timeToDouble(millis: Long): Double = millis.toDouble / 1000000000.0

  /**
   * Adjust the `weightage` of current time by raising it to power of [[currentTimeAdjustmentExp]].
   */
  private def millisTimeAdjusted(timeInMillis: Long): Double = {
    // recalculate currentTimeAdjustmentExp every hour
    if (timeInMillis - lastCurrentTimeAdjustment > 3600000L) {
      evictionLock.lockInterruptibly()
      try {
        lastCurrentTimeAdjustment = timeInMillis
        currentTimeAdjustmentExp = calcCurrentTimeAdjustment(timeInMillis)
      } finally {
        evictionLock.unlock()
      }
    }
    math.pow(timeToDouble(timeInMillis), currentTimeAdjustmentExp)
  }

  /**
   * Compression of an object can provide considerable savings in memory. However, it also
   * has the overhead of decompression. This is a quick way to determine what is better using
   * the [[decompressionToDiskReadCost]] field.
   */
  private def addCompressionSavingsToWeightage(
      currentMillisAdjusted: Double,
      compressed: CompressedCacheObject[C, D],
      compressedSize: Long,
      decompressedSize: Long): Unit = {
    val compressionSavingsFactor = calcCompressionSavingsFactor(compressedSize, decompressedSize)
    if (compressionSavingsFactor > 0.0) {
      compressed.compressionSavings = currentMillisAdjusted * compressionSavingsFactor
      compressed.weightage += compressed.compressionSavings
    }
  }

  /**
   * Calculate the factor to apply for compression savings by [[addCompressionSavingsToWeightage]].
   */
  private def calcCompressionSavingsFactor(
      compressedSize: Long,
      decompressedSize: Long): Double = {
    ((decompressedSize.toDouble / compressedSize.toDouble) *
      (1.0 - decompressionToDiskReadCost)) - 1.0
  }

  private def release(value: CacheValue, cached: StoredCacheObject[_ <: CacheValue]): Unit = {
    assert(value ne null)
    usedMemory.addAndGet(-value.memorySize)
    value.release()
    // moved to evictedEntries map
    if (cached ne null) {
      cached.clearValue()
      evictedEntries.put(cached.key, cached)
      purgeEvictedEntriesIfRequired()
    }
  }

  private def purgeEvictedEntriesIfRequired(): Boolean = {
    if (evictedEntries.size() > maxEvictedEntries) {
      val iter = evictedEntries.values().iterator()
      while (iter.hasNext) {
        if (canPurge(iter.next())) {
          iter.remove()
          if (evictedEntries.size() <= maxEvictedEntries) return true
        }
      }
      true
    } else false
  }

  private def canPurge(cached: StoredCacheObject[_ <: CacheValue]): Boolean = cached match {
    // cannot remove a valid WeakReference else finalizeReferent() can be skipped
    case s: StoredCacheObjectWithFinalization[_] if s.get() ne null => false
    case _ => true
  }

  override def setLimit(newMaxMemory: Long, timestamp: Long): Unit = {
    maxMemoryLock.writeLock().lockInterruptibly()
    try {
      maxMemory = newMaxMemory
      runEviction(blocking = true, pendingEntry = null, timestamp)
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putObject(
      key: Comparable[AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long): Boolean = {
    cacheObject(key, either, transformer, timestamp, throwOnLargeObject = true)
  }

  private def cacheObject(
      key: Comparable[AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long,
      throwOnLargeObject: Boolean): Boolean = {

    val value = either match {
      case Left(v) => v
      case Right(v) => v
    }
    val memorySize = value.memorySize
    // basic requirements
    require(key != null)
    require(either.isLeft || !value.isCompressed)
    require(either.isRight || value.isCompressed)
    require(memorySize > 0L)
    require(transformer ne null)
    require(transformer.compressionAlgorithm ne null)
    // invariant to check for case when there are no compressed/decompressed versions of the object
    require(transformer.compressionAlgorithm.isDefined || !value.isCompressed)

    maxMemoryLock.readLock().lockInterruptibly()
    try {
      if (memorySize >= maxMemory) {
        if (throwOnLargeObject) {
          throw new IllegalArgumentException(
            s"Cannot put object of $memorySize bytes with maxMemory = $maxMemory")
        } else return false
      }
      val cached = StoredCacheObject(key, either, transformer)
      cached.weightage = millisTimeAdjusted(timestamp)
      // if this is a compressed object, then add the savings due to compression to weightage
      if (value.isCompressed) {
        addCompressionSavingsToWeightage(
          cached.weightage,
          cached.asInstanceOf[CompressedCacheObject[C, D]],
          memorySize,
          transformer.decompressedSize(either.asInstanceOf[Left[C, D]].value))
      }
      // quick check to determine if the object being inserted has a lower weightage than
      // the smallest one in the evictionMap
      if (evictionMap.firstKey().weightage < cached.weightage) {
        putStoredObject(key, value, cached.asInstanceOf[StoredCacheObject[CacheValue]], timestamp)
      } else false
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  private def putStoredObject[T <: CacheValue](
      key: Comparable[AnyRef],
      value: T,
      storeObject: StoredCacheObject[T],
      timestamp: Long): Boolean = {
    value.use() // value will be put in cache so mark as in-use
    usedMemory.addAndGet(value.memorySize)
    val pendingEntry = new CachedPending[T](value, storeObject, timestamp)
    cacheAndEvict(key, pendingEntry)
    pendingEntry.cached // returns valid value if cacheAndEvict was run synchronously
  }

  override def getDecompressed(
      key: Comparable[AnyRef],
      timestamp: Long,
      loader: Option[Comparable[AnyRef] => Option[(Either[C, D], TransformValue[C, D])]])
    : Option[D] = {

    var cached = cacheMap.get(key)
    var cachedValue: CacheValue = if (cached ne null) cached.value else null
    if (cachedValue eq null) {
      // lookup in the pending cache
      val pendingEntry = pendingWorkItems.get(key)
      if (pendingEntry ne null) {
        cached = pendingEntry.storeObject
        if (cached ne null) cachedValue = cached.value
      }
    }
    // increment reference count for the value that can be returned with tryUse()
    // since it is possible in the worst case that the object was just evicted and released
    if ((cachedValue ne null) && !cachedValue.tryUse()) {
      cachedValue = null
    }
    if (cachedValue ne null) cached match {
      case d: DecompressedCacheObject[D, C] =>
        // update weightage for access and need to remove+put into evictionMap to reorder
        maxMemoryLock.readLock().lockInterruptibly()
        try {
          cacheAndEvict(key, new UpdateWeightageForAccess(d, timestamp))
        } finally {
          maxMemoryLock.readLock().unlock()
        }
        Some(cachedValue.asInstanceOf[D])

      case c: CompressedCacheObject[C, D] =>
        val (d, dv) = c.decompress(cachedValue.asInstanceOf[C])
        cachedValue.release() // release the extra reference count added by tryUse() before
        val dvSize = dv.memorySize
        // determine if decompressed object should be cached immediately
        val cw = c.weightage
        d.weightage = cw - c.compressionSavings + millisTimeAdjusted(timestamp)
        // quick check to determine if the object being inserted has a lower weightage than
        // the smallest one in the evictionMap; in the worst case this can still end up
        // putting decompressed object in cache while runEviction can remove it or
        // change to compressed form again
        if (d.weightage >= cw || evictionMap.firstKey().weightage < d.weightage) {
          maxMemoryLock.readLock().lockInterruptibly()
          try {
            if (dvSize < maxMemory) putStoredObject(key, dv, d, timestamp)
          } finally {
            maxMemoryLock.readLock().unlock()
          }
        }
        dv.use()
        Some(dv)
    } else {
      loader match {
        case Some(l) =>
          l(key) match {
            case Some((either, transformer)) =>
              // return the loaded object decompressing, if required, and caching the result
              either match {
                case Left(value) =>
                  value.use()
                  try {
                    val decompressed = transformer.decompress(value)
                    val right = Right[C, D](decompressed)
                    cacheObject(key, right, transformer, timestamp, throwOnLargeObject = false)
                    decompressed.use()
                    Some(decompressed)
                  } finally {
                    value.release()
                  }
                case Right(value) =>
                  cacheObject(key, either, transformer, timestamp, throwOnLargeObject = false)
                  value.use()
                  Some(value)
              }
            case _ => None
          }

        case _ => None
      }
    }
  }

  override def removeObject(key: Comparable[AnyRef]): Boolean = {
    val removeCached = new RemoveCached[CacheValue](key, System.currentTimeMillis())
    runEviction(blocking = true, removeCached, removeCached.timestamp)
    removeCached.removed
  }

  override def checkAndForceConsistency(): Boolean = {
    maxMemoryLock.writeLock().lock()
    evictionLock.lock()
    try {
      var consistent = true
      val evictionIter = evictionMap.entrySet().iterator()
      while (evictionIter.hasNext) {
        val entry = evictionIter.next()
        val storedObject = entry.getKey
        val key = storedObject.key
        val value = entry.getValue
        val storedValue = storedObject.value
        if ((value eq null) || (storedValue eq null) || (value ne storedValue) || !value.isInUse) {
          cacheMap.remove(key)
          evictionIter.remove()
          consistent = false
        } else {
          val cacheMapValue = cacheMap.get(key)
          if (storedObject ne cacheMapValue) {
            cacheMap.remove(key)
            evictionIter.remove()
            consistent = false
          } else {
            // valid entry
            if (evictedEntries.remove(key) ne null) consistent = false
          }
        }
      }

      var used = 0L
      val cacheIter = cacheMap.entrySet().iterator()
      while (cacheIter.hasNext) {
        val entry = cacheIter.next()
        val key = entry.getKey
        val storedObject = entry.getValue
        val storedValue = storedObject.value
        if ((storedValue eq null) || key != storedObject.key) {
          cacheIter.remove()
          evictionMap.remove(storedObject)
          consistent = false
        } else if (!evictionMap.containsKey(storedObject)) {
          cacheIter.remove()
          consistent = false
        } else {
          // valid entry
          used += storedValue.memorySize
        }
      }
      if (used != usedMemory.get()) {
        usedMemory.set(used)
        consistent = false
      }

      if (purgeEvictedEntriesIfRequired()) consistent = false
      if (evictedEntries.size() > maxEvictedEntries) {
        System.gc()
        if (purgeEvictedEntriesIfRequired()) consistent = false
      }

      assert(cacheMap.size() == evictionMap.size())

      consistent
    } finally {
      evictionLock.unlock()
      maxMemoryLock.writeLock().unlock()
    }
  }

  /**
   * Convenience method to either perform caching related work followed by eviction, if required,
   * or else push the work item into [[pendingWorkItems]].
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] has been acquired
   * before invoking this method.
   */
  private def cacheAndEvict[T <: CacheValue](
      key: Comparable[AnyRef],
      pendingEntry: PendingEntry[T]): Unit = {
    val timestamp = pendingEntry.timestamp
    if (runEviction(blocking = false, pendingEntry, timestamp) == Long.MinValue) {
      // check if usedMemory is over maxMemory by too much
      val used = usedMemory.get()
      if (used.toDouble >= (1.0 + maxMemoryOverflowBeforeEvict) * maxMemory.toDouble) {
        runEviction(blocking = true, pendingEntry, timestamp)
      } else {
        enqueueWork(key, pendingEntry)
      }
    }
  }

  private def enqueueWork[T <: CacheValue](
      key: Comparable[AnyRef],
      pendingEntry: PendingEntry[T]): Unit = {
    pendingWorkLock.readLock().lockInterruptibly()
    try {
      pendingWorkItems.put(key, pendingEntry)
    } finally {
      pendingWorkLock.readLock().unlock()
    }
  }

  /**
   * Run the eviction loop in order to bring [[usedMemory]] below [[maxMemory]] and clear up any
   * work items in [[pendingWorkItems]]. Only one thread can perform eviction at a time and
   * in order to minimize the bottleneck of multiple threads waiting for [[runEviction]], the
   * other threads should queue up their work in [[pendingWorkItems]] as long as [[usedMemory]]
   * has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] (for the latter
   * case the thread will need to either wait for [[runEviction]] to complete or skip caching).
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] or [[maxMemoryLock.writeLock()]]
   * has been acquired before invoking this method.
   *
   * @param blocking when true then [[evictionLock]] is acquired in a blocking manner else a
   *                 `tryLock` with zero timeout is attempted
   * @param pendingEntry the work required to be done by current thread if [[evictionLock]] was
   *                     acquired, else caller is supposed to queue it in [[pendingWorkItems]]
   *                     and let the other thread that successfully acquires it execute the method
   * @param timestamp the current timestamp which should be the [[System.currentTimeMillis()]]
   *                  at the start of operation
   * @param maxFlushPending skip and block flushPendingWork after these many iterations
   *
   * @return if [[evictionLock.tryLock]] was acquired then the number of bytes evicted else
   *         [[Long.MinValue]] if eviction was skipped (in which case the caller is supposed
   *         to queue the `pendingEntry` in [[pendingWorkItems]] if the total used memory
   *         has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]]
   */
  private def runEviction[T <: CacheValue](
      blocking: Boolean,
      pendingEntry: PendingEntry[T],
      timestamp: Long,
      maxFlushPending: Int = 20): Long = {
    if (blocking) evictionLock.lockInterruptibly()
    else if (!evictionLock.tryLock()) {
      return Long.MinValue // indicates that runEviction was skipped
    }
    val pendingCompressedObjects = new java.util.ArrayList[(CompressedCacheObject[C, D], C)]()
    var remainingFlushes = maxFlushPending
    val max = maxMemory

    try {
      flushPendingWork(remainingFlushes == 0)
      remainingFlushes -= 1
      if (pendingEntry ne null) runIgnoreException(pendingEntry)
      if (max >= usedMemory.get()) {
        flushPendingWork(remainingFlushes == 0)
        remainingFlushes -= 1
        return 0L
      }

      var evicted = 0L
      var iter = evictionMap.keySet().iterator()
      while (iter.hasNext) {
        val candidate = iter.next()
        val removeKey = candidate.key
        val removedVal = candidate.value
        assert(removedVal ne null)
        if (remainingFlushes >= 0 && flushPendingWork(remainingFlushes == 0)) {
          remainingFlushes -= 1
          // evictionMap changed so restart the iterator
          iter = evictionMap.keySet().iterator()
        } else {
          assert(cacheMap.remove(removeKey) eq candidate)
          iter.remove()
          var removedSize = removedVal.memorySize
          val currentMillisAdjusted = millisTimeAdjusted(timestamp)
          val transformer =
            candidate.transformer.asInstanceOf[TransformValue[CacheValue, CacheValue]]
          // for decompressed blocks, compress and put them back rather than evicting entirely
          if (!candidate.isCompressed && transformer.compressionAlgorithm.isDefined &&
              evictionMap.firstKey().weightage < (candidate.weightage + currentMillisAdjusted *
                calcCompressionSavingsFactor(
                  transformer.compressedSize(removedVal),
                  removedSize))) {
            val p @ (compressed, compressedVal) = candidate
              .asInstanceOf[DecompressedCacheObject[D, C]]
              .compress(removedVal.asInstanceOf[D])
            val compressedSize = compressedVal.memorySize
            compressed.weightage = candidate.weightage
            addCompressionSavingsToWeightage(
              currentMillisAdjusted,
              compressed,
              compressedSize,
              removedSize)
            compressedVal.use() // value can be put in cache so mark as in-use
            if (compressed.weightage > candidate.weightage) {
              usedMemory.addAndGet(compressedSize)
              pendingCompressedObjects.add(p)
              removedSize -= compressedSize
            } else compressedVal.release()
          }
          release(removedVal, candidate)
          evicted += removedSize
          // break the loop if usedMemory has fallen below maxMemory
          if (max >= usedMemory.get()) return evicted
        }
      }
      // insert the pending compressed objects and try again
      if (flushPendingCompressedObjects(pendingCompressedObjects)) {
        evicted + runEviction(blocking = true, pendingEntry = null, timestamp, remainingFlushes)
      } else evicted
    } finally {
      try {
        flushPendingCompressedObjects(pendingCompressedObjects)
      } finally {
        evictionLock.unlock()
        if (remainingFlushes < 0 && maxFlushPending >= 0) {
          pendingWorkLock.writeLock().unlock()
        }
      }
    }
  }

  private def runIgnoreException(runnable: Runnable): Unit = {
    try {
      runnable.run()
    } catch {
      case t: Throwable =>
        try {
          // Continue despite fatal exceptions like OutOfMemory because further operations can
          // release memory and lead to a functioning JVM. In the worst case if the JVM has really
          // become unworkable, most operations will start throwing OOMEs and JVM can become stuck
          // in messaging etc which should be properly handled at the Spark application layer
          // (e.g. using a JNI module that kills the JVM if it is stuck for a long time).
          logError("Unexpected exception in map operations", t)
        } catch {
          case _: Throwable => // ignored
        }
    }
  }

  /*
  private def isNonFatalException(t: Throwable): Boolean = {
    !t.isInstanceOf[OutOfMemoryError] || t.isInstanceOf[SparkOutOfMemoryError] ||
    t.getMessage.contains("Direct buffer") // direct buffer allocation failures are non-fatal
  } */

  private def putIntoCache[T <: CacheValue](storeObject: StoredCacheObject[T], value: T): Unit = {
    val oldCached = cacheMap.put(storeObject.key, storeObject)
    if (oldCached ne null) release(evictionMap.remove(oldCached), oldCached)
    storeObject match {
      case ref: StoredCacheObjectWithFinalization[T] => value.finalizer = ref
      case _ =>
    }
    assert(evictionMap.putIfAbsent(storeObject, value) eq null)
  }

  private def flushPendingWork(blocking: Boolean): Boolean = {
    // avoid locking the queue for entire duration since each work item in the queue can be
    // expensive, so block only when beyond a count or explicit flag has been passed
    if (blocking) {
      pendingWorkLock.writeLock().lock()
      val numItems = pendingWorkItems.size()
      runIgnoreException(() => flushAllPendingWork())
      numItems > 0
    } else {
      var numItems = 0
      val iter = pendingWorkItems.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val value = entry.getValue
        runIgnoreException(value)
        pendingWorkItems.remove(entry.getKey, value)
        numItems += 1
        if (numItems >= 20) { // blocking run
          pendingWorkLock.writeLock().lockInterruptibly()
          try {
            flushAllPendingWork()
          } finally {
            pendingWorkLock.writeLock().unlock()
          }
        }
      }
      numItems > 0
    }
  }

  private def flushAllPendingWork(): Unit = {
    val iter = pendingWorkItems.values().iterator()
    while (iter.hasNext) {
      runIgnoreException(iter.next())
    }
    pendingWorkItems.clear()
  }

  private def flushPendingCompressedObjects(
      objects: java.util.ArrayList[(CompressedCacheObject[C, D], C)]): Boolean = {
    val numObjects = objects.size()
    if (numObjects != 0) {
      var i = 0
      while (i < numObjects) {
        val p = objects.get(i)
        i += 1
        runIgnoreException(() => putIntoCache(p._1, p._2))
      }
      objects.clear()
      true
    } else false
  }

  private sealed abstract class PendingEntry[T <: CacheValue] extends Runnable {
    def storeObject: StoredCacheObject[T]
    def timestamp: Long
  }

  private final class CachedPending[T <: CacheValue](
      value: T,
      override val storeObject: StoredCacheObject[T],
      override val timestamp: Long)
      extends PendingEntry[T] {

    // assume true by default in case task is to be queued and run asynchronously
    private[this] var _cached = true

    def cached: Boolean = _cached

    override def run(): Unit = {
      // only cache if the weightage is at least more than the entry with smallest weightage
      if (evictionMap.firstKey().weightage < storeObject.weightage) {
        putIntoCache(storeObject, value)
        _cached = true
      } else {
        release(value, storeObject)
        _cached = false
      }
    }
  }

  private final class UpdateWeightageForAccess[T <: CacheValue](
      override val storeObject: StoredCacheObject[T],
      override val timestamp: Long)
      extends PendingEntry[T] {

    override def run(): Unit = {
      val value = evictionMap.remove(storeObject)
      if (value ne null) {
        storeObject.weightage += millisTimeAdjusted(timestamp)
        assert(evictionMap.putIfAbsent(storeObject, value) eq null)
      }
    }
  }

  private final class RemoveCached[T <: CacheValue](
      key: Comparable[AnyRef],
      override val timestamp: Long)
      extends PendingEntry[T] {

    private[this] var _removed = false

    def removed: Boolean = _removed

    override def storeObject: StoredCacheObject[T] = null // indicates that key is to be removed

    override def run(): Unit = {
      val cached = cacheMap.remove(key)
      if (cached ne null) {
        release(evictionMap.remove(cached), cached)
        _removed = true
      } else {
        _removed = false
      }
      evictedEntries.computeIfPresent(key, (_, v) => if (canPurge(v)) null else v)
    }
  }
}
