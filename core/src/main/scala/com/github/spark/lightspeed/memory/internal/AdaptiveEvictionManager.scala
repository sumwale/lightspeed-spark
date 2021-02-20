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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.github.spark.lightspeed.memory._
import com.github.spark.lightspeed.util.Collections

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
 * 1) Getter will try to cache the decompressed version, if not present. So decompressed objects
 *    will keep filling memory till memory limit is not reached.
 * 2) Evaluate an estimate for savings due to compression and add it to the `weightage`.
 *    This estimate uses a provided `decompressionToDiskReadCost` parameter which is the ratio
 *    of the cost of decompression of an object to reading from disk. The rationale for this
 *    and some other thoughts are in the notes later in this comment.
 * 3) Once the limit is reached, eviction will kick in to check if compressing the `oldest`
 *    decompressed object is more efficient than evicting it (see more in the notes later).
 * 4) If eviction hits on a compressed object to evict rather than a decompressed one, then
 *    always evict it as usual. The `weightage` of compressed objects is already increased
 *    as per the savings due to compression as noted in point 2 (see more in the notes later).
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
 *                           as two accesses as of now; this should not be smaller than 10 secs
 *                           and recommended to be 5 minutes or more else the exponent required
 *                           for weightage becomes too large
 * @param decompressionToDiskReadCost ratio of cost to decompress a disk block to reading it
 *                                    from disk (without OS caching)
 * @param maxOverflowPercent maximum ratio of `maxMemorySize` that can overflow before eviction
 *                           would forcefully block putter/getter threads
 * @param maxEvictedEntries maximum count of old evicted entries before all their stats are purged
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
final class AdaptiveEvictionManager[C <: CacheValue, D <: CacheValue](
    maxMemorySize: Long,
    millisForTwiceFreq: Long,
    decompressionToDiskReadCost: Double,
    maxOverflowPercent: Double,
    maxEvictedEntries: Int)
    extends EvictionManager[C, D]
    with Logging {

  def this(maxMemorySize: Long, millisForTwiceFreq: Long) = {
    this(
      maxMemorySize,
      millisForTwiceFreq,
      decompressionToDiskReadCost = 0.2,
      maxOverflowPercent = 0.05,
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
    maxOverflowPercent > 0.0 && maxOverflowPercent < 0.5,
    "maximum ratio that can overflow before blocking for eviction should be (0.0, 0.5) " +
      s"but was $maxOverflowPercent")
  require(maxEvictedEntries > 0, s"maximum count of evicted entries is $maxEvictedEntries")

  /**
   * The current value of the upper limit on memory in bytes allowed.
   * Should always be accessed/updated within [[maxMemoryLock]].
   */
  private[this] var maxMemory: Long = maxMemorySize

  /**
   * Lock used to access/update [[maxMemory]]. The [[setLimit]] operation acquires the write lock
   * while all other operations that intend to read [[maxMemory]] and depend on checking the limit
   * should acquire the read lock for the duration of the operation.
   */
  private[this] val maxMemoryLock = new ReentrantReadWriteLock()

  /**
   * This should accurately track the total size of [[CacheValue]]s in [[cacheMap]] as well as
   * [[pendingActions]].
   */
  private[this] val usedMemory = new AtomicLong(0L)

  /**
   * Lock acquired by [[runEviction]] for synchronization.
   * Used instead of a synchronized block to enable use of tryLock().
   */
  private[this] val evictionLock = new ReentrantLock()

  /**
   * The main map that keeps the cached [[StoredCacheObject]] for the given key. Reads will
   * always consult this map to return the cached value else invoke loader if provided.
   */
  private[this] val cacheMap =
    new ConcurrentHashMap[Comparable[_ <: AnyRef], StoredCacheObject[_ <: CacheValue]]()

  /**
   * The set used for ordering the cached [[StoredCacheObject]]s by their `weightage` and uses
   * that order to evict those with smallest weightage when [[usedMemory]] exceeds [[maxMemory]].
   *
   * This should always be consistent with [[cacheMap]] in that the [[StoredCacheObject]] in the
   * [[cacheMap]] against a key should be the key in [[evictionSet]]. If this invariant fails then
   * the cache can end up having two [[CacheValue]]s stored against the same key and/or two
   * [[StoredCacheObject]]s for the same key and lead to a lot of trouble. This invariant is
   * ensured by having only a single thread update the two maps during [[runEviction]] under the
   * [[evictionLock]] and is asserted by the implementations of [[PendingAction]]. Readers only
   * consult [[cacheMap]] so there is no problem of inconsistency for them.
   */
  @GuardedBy("evictionLock")
  private[this] val evictionSet =
    new ConcurrentSkipListSet[StoredCacheObject[_ <: CacheValue]](
      (o1: StoredCacheObject[_ <: CacheValue], o2: StoredCacheObject[_ <: CacheValue]) => {
        if (o1 ne o2) {
          val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
          if (cmp != 0) cmp
          else {
            val c = o1.key.asInstanceOf[Comparable[AnyRef]].compareTo(o2.key.asInstanceOf[AnyRef])
            // order compressed having same key to be lower priority than decompressed
            if (c != 0) c else java.lang.Boolean.compare(!o1.isCompressed, !o2.isCompressed)
          }
        } else 0
      })

  /**
   * These are the entries thrown out of [[cacheMap]] and [[evictionSet]] by eviction but
   * are retained for future statistics. When evicted entries are "resurrected" by a future
   * [[putObject]] or [[getDecompressed]] operation, then the retained `weightage` is added to
   * the new object's `weightage` and also used for [[StoredCacheObject.generation]] handling.
   *
   * This map is only cleared when the count of entries exceeds the provided [[maxEvictedEntries]]
   * (the `LRUHashMap` is used for the same purpose to purge oldest changed entries first).
   */
  private[this] val evictedEntries =
    Collections.newLRUMap[Comparable[_ <: AnyRef], CacheValueStats](maxEvictedEntries)

  /**
   * Map to hold pending caching work. Callers should ensure that [[usedMemory]] should not exceed
   * [[maxMemory]] by more than [[maxOverflowPercent]] before pushing new work items
   * into this map else thread should either fail the caching or block on [[runEviction]].
   */
  private[this] val pendingActions =
    new ConcurrentHashMap[Comparable[_ <: AnyRef], PendingAction[_ <: CacheValue]]()

  /**
   * A read-write lock for [[pendingActions]]. Threads that are reading or pushing new work
   * items should acquire the read lock so that all of them can proceed concurrently, while
   * the eviction thread can acquire the write lock when draining the map. This is required
   * so that the eviction thread can finish its work and not keep iterating possibly
   * indefinitely as new work items keep being pushed continuously.
   */
  private[this] val pendingActionLock = new ReentrantReadWriteLock()

  /**
   * Temporarily stores all the flushed [[StoredCacheObject]]s in [[pendingActions]] during
   * [[runEviction]]. Currently used to see which generation objects evict which ones, and apply
   * a boost as per the `generation` if appropriate to the retained entries.
   */
  @GuardedBy("evictionLock")
  private[this] val currentFlushedEntries =
    Collections.newOpenHashSet[StoredCacheObject[_ <: CacheValue]]()

  /**
   * Instance of [[CurrentTimeAdjustment]] used to calculate [[millisTimeAdjusted]]. Not volatile
   * nor any lock used when reading since reading/writing somewhat stale value does not matter.
   */
  private[this] var currentTimeAdjustment =
    new CurrentTimeAdjustment(millisForTwiceFreq, System.currentTimeMillis())

  /**
   * Adjust the `weightage` of provided time using [[CurrentTimeAdjustment.millisTimeAdjusted]].
   */
  private def millisTimeAdjusted(timeInMillis: Long): Double = {
    val timeAdjustment = currentTimeAdjustment
    // recalculate CurrentTimeAdjustment every hour
    if (!timeAdjustment.needsRefresh(timeInMillis)) {
      timeAdjustment.millisTimeAdjusted(timeInMillis)
    } else {
      // no synchronization or volatile since multiple threads setting similar value
      // concurrently, overriding others only once in an hour is only a minuscule waste of effort
      val newTimeAdjustment = new CurrentTimeAdjustment(millisForTwiceFreq, timeInMillis)
      currentTimeAdjustment = newTimeAdjustment
      newTimeAdjustment.millisTimeAdjusted(timeInMillis)
    }
  }

  /**
   * Release a value that was in the cache that is assumed to be removed from [[cacheMap]] and
   * [[evictionSet]] prior to this method being invoked. This does [[usedMemory]] bookkeeping,
   * [[FinalizeValue]] handling of the value and adds to [[evictedEntries]].
   *
   * @param cached the [[StoredCacheObject]] removed from the cache
   * @param isEvicted if true then add statistics to [[evictedEntries]] else skip it
   */
  private def release(cached: StoredCacheObject[_ <: CacheValue], isEvicted: Boolean): Unit = {
    val value = cached.value
    usedMemory.addAndGet(-value.memorySize)
    if (!value.release()) {
      // other references exist so add finalizer to CacheValue and EvictionService's map
      cached.transformer.addFinalizerIfMissing(value)
    }
    if (isEvicted) {
      // add decompressed value statistics to evictedEntries
      evictedEntries.put(cached.key, cached.toStats)
    }
  }

  /**
   * Return smallest weightage in [[evictionSet]] or 0.0 if set is empty.
   */
  private def smallestWeightage(): Double = {
    try {
      evictionSet.first().weightage
    } catch {
      case _: NoSuchElementException => 0.0
    }
  }

  /**
   * A quick check to determine if a given [[StoredCacheObject]] should be put in cache using
   * currently available memory and the [[smallestWeightage]].
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock]] or [[maxMemoryLock.writeLock]]
   *       has been acquired before invoking this method.
   *
   * @param storeObject the [[StoredCacheObject]] to check
   * @param memorySize size in bytes of `storeObject` that will be added to [[usedMemory]] if it
   *                   is actually cached
   */
  private def shouldBeCached(
      storeObject: StoredCacheObject[_ <: CacheValue],
      memorySize: Long): Boolean = {
    maxMemory >= (usedMemory.get() + memorySize) || smallestWeightage() < storeObject.weightage
  }

  override def setLimit(newMaxMemory: Long, timestamp: Long): Unit = {
    maxMemoryLock.writeLock().lockInterruptibly()
    try {
      maxMemory = newMaxMemory
      runEviction(blocking = true, millisTimeAdjusted(timestamp))
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putObject(
      key: Comparable[_ <: AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long): Boolean = {

    cacheObject(key, either, transformer, timestamp, fromPut = true)
  }

  /**
   * Like [[putObject]] but adds a parameter `fromPut` that will skip throwing
   * [[UnsupportedOperationException]] if it is false and value size is greater than total memory.
   * It will also immediately release on-the-fly value that does not get cached i.e. when `fromPut`
   * is false. This method is used to cache an object by [[putObject]] and by
   * [[getDecompressed]] for cache miss.
   *
   * @param fromPut true if invoked from [[putObject]] else false when invoked for internally
   *                created objects (e.g. from loader or decompressed version of cached object)
   * @param existing if caching decompressed version of a cached compressed object, then this is
   *                 the existing compressed object read from cache
   *
   * @see [[putObject]]
   */
  private def cacheObject(
      key: Comparable[_ <: AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long,
      fromPut: Boolean,
      existing: Option[CompressedCacheObject[C, D]] = None): Boolean = {

    val value = either match {
      case Left(v) => v
      case Right(v) => v
    }

    var cachingStarted = false
    maxMemoryLock.readLock().lockInterruptibly()
    try {
      // basic requirements
      require(key != null)
      require(value ne null)

      val memorySize = value.memorySize

      // other requirements
      require(value.isValid)
      require(either.isLeft || !value.isCompressed)
      require(either.isRight || value.isCompressed)
      require(memorySize > 0L)
      require(transformer ne null)
      require(transformer.compressionAlgorithm ne null)

      // invariant to check when there are no compressed/decompressed versions of the object
      require(transformer.compressionAlgorithm.isDefined || !value.isCompressed)

      if (!fromPut) {
        // For values from loader or intermediate results, increase referenceCount since this
        // will be sent outside to caller and so should have one referenceCount incremented
        // as per the contract of getDecompressed(). This is done before any return, and if the
        // use() calls throws exception then it will be handled cleanly in the catch block.
        value.use()
      }
      if (maxMemorySize > maxMemory) {
        if (fromPut) {
          throw new UnsupportedOperationException(
            s"Cannot put object of $memorySize bytes with maxMemory = $maxMemory")
        } else {
          // value can be visible outside without being in cache so add finalizer for cleanup
          transformer.addFinalizerIfMissing(value)
          return false
        }
      }
      val timeWeightage = millisTimeAdjusted(timestamp)
      val storeObject = Utils.newStoredCacheObject(
        key,
        value,
        transformer,
        timeWeightage,
        decompressionToDiskReadCost)

      existing match {
        case Some(c) =>
          storeObject.weightage += (c.weightageWithoutBoost - c.compressionSavings)
          storeObject.generation = c.generation
        case _ =>
          val evicted = evictedEntries.get(key)
          if (evicted ne null) {
            // add the old statistical weightage which is always for decompressed object since
            // compressionSavings is already present in storeObject is required
            storeObject.weightage += evicted.weightage
            // evicted object being resurrected so the generation will increase
            storeObject.generation = evicted.generation + 1
          }
      }
      // quick check to determine if the object being inserted has a lower weightage than
      // the smallest one in the evictionSet which is fine for this case since the key cannot
      // be in evictedEntries so the weightage so far is accurate; in the worst case this can
      // still end up putting decompressed object in cache while runEviction can remove it or
      // change to compressed form again but that should be very rare
      if (shouldBeCached(storeObject, memorySize)) {
        value.use() // value can be put in cache so mark as in-use
        usedMemory.addAndGet(memorySize)
        cachingStarted = true
        cacheAndEvict(key, new CachePending(storeObject, timeWeightage), timeWeightage)
        true
      } else {
        // update statistics for the key if present
        evictedEntries.update(key, (_, v) => v.addWeightage(timeWeightage))
        // value can be visible outside without being in cache so add finalizer for cleanup
        transformer.addFinalizerIfMissing(value)
        false
      }
    } catch {
      case t: Throwable =>
        if (cachingStarted) {
          // revert the changes done before cacheAndEvict failed
          usedMemory.addAndGet(-value.memorySize)
          value.release()
        }
        if (fromPut) {
          // value can be visible outside without being in cache so add finalizer for cleanup
          transformer.addFinalizerIfMissing(value)
        } else {
          value.finalizeSelf() // intermediate result will be lost so directly free it
        }
        throw t
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  /**
   * Update the `weightage` of an existing [[StoredCacheObject]] for read using given timestamp.
   */
  private def touchObject[T <: CacheValue](
      key: Comparable[_ <: AnyRef],
      storeObject: StoredCacheObject[T],
      timestamp: Long): Unit = {
    val timeWeightage = millisTimeAdjusted(timestamp)
    maxMemoryLock.readLock().lockInterruptibly()
    try {
      // update weightage for access and need to remove+put into evictionSet to reorder
      cacheAndEvict(key, new UpdateWeightageForAccess(storeObject, timeWeightage), timeWeightage)
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  override def getDecompressed(
      key: Comparable[_ <: AnyRef],
      timestamp: Long,
      loader: Option[Comparable[_ <: AnyRef] => Option[(Either[C, D], TransformValue[C, D])]])
    : Option[D] = {

    var cached = cacheMap.get(key)
    if (cached eq null) {
      // lookup in the pending cache
      val pendingAction = pendingActions.get(key)
      if (pendingAction ne null) cached = pendingAction.storeObject
    }
    var cachedValue: CacheValue = if (cached ne null) cached.value else null
    // increment reference count for the value that can be returned with tryUse()
    // since it is possible in the worst case that the object was just evicted and released
    if ((cachedValue ne null) && !cachedValue.tryUse()) {
      cachedValue = null
    }
    if (cachedValue ne null) cached match {
      case d: DecompressedCacheObject[D, C] =>
        var success = false
        try {
          touchObject(key, d, timestamp)
          success = true
          Some(d.value)
        } finally {
          // release the extra reference count added by tryUse() before
          if (!success) cachedValue.release()
        }

      case c: CompressedCacheObject[C, D] =>
        try {
          val transformer = c.transformer.asInstanceOf[TransformValue[C, D]]
          val decompressedValue = transformer.decompress(cachedValue.asInstanceOf[C])
          val right = Right[C, D](decompressedValue)
          // if decompressed value cannot be cached then touch the existing value
          if (!cacheObject(key, right, transformer, timestamp, fromPut = false, Some(c))) {
            touchObject(key, c, timestamp)
          }
          Some(decompressedValue)
        } finally {
          // release the extra reference count added by tryUse() before
          cachedValue.release()
        }
    } else {
      loader match {
        case Some(l) =>
          l(key) match {
            case Some((either, transformer)) =>
              // return the loaded object decompressing, if required, and caching the result
              either match {
                case Left(compressed) =>
                  var cached = false
                  try {
                    val decompressed = transformer.decompress(compressed)
                    val right = Right[C, D](decompressed)
                    // cache decompressed value and if that fails then cache compressed value
                    cached = !cacheObject(key, right, transformer, timestamp, fromPut = false) &&
                      cacheObject(key, either, transformer, timestamp, fromPut = false)
                    Some(decompressed)
                  } finally {
                    if (!cached) compressed.finalizeSelf()
                  }
                case Right(decompressed) =>
                  cacheObject(key, either, transformer, timestamp, fromPut = false)
                  Some(decompressed)
              }
            case _ => None
          }

        case _ => None
      }
    }
  }

  /**
   * Run some action in a blocking way without doing any eviction. This will block against
   * [[runEviction]] and if `maxMemoryWriteLocked` is true then all other operations will
   * also be blocked till the end of `action` since they acquire the [[maxMemoryLock.readLock]].
   */
  private def runBlocking[T](action: => T, maxMemoryWriteLocked: Boolean = false): T = {
    var evictionLocked = false
    val mLock = if (maxMemoryWriteLocked) maxMemoryLock.writeLock() else maxMemoryLock.readLock()
    mLock.lockInterruptibly()
    try {
      evictionLock.lockInterruptibly()
      evictionLocked = true

      action

    } finally {
      if (evictionLocked) evictionLock.unlock()
      mLock.unlock()
    }
  }

  override def getStatistics(predicate: Comparable[_ <: AnyRef] => Boolean): StatisticsMap = {
    val map = Collections.newOpenHashMap[Comparable[_ <: AnyRef], CacheValueStats]

    // No evictionLock acquired to block operations since minor discrepancies in statistics
    // are fine. Usual cases for this method being invoked include the case when a Spark partition
    // execution moves to another node in which case the effected keys will not be operated on.

    // iterate evictedEntries first since those are lowest priority and can be overwritten later
    evictedEntries.foreach((k, v) => { if (predicate(k)) assert(map.put(k, v) eq null); true })

    // next the cacheMap entries
    val cachedIter = cacheMap.entrySet().iterator()
    while (cachedIter.hasNext) {
      val entry = cachedIter.next()
      val key = entry.getKey
      if (predicate(key)) map.put(key, entry.getValue.toStats)
    }
    // last are the pendingActions which are highest priority adjusting weightage if the key
    // is already present in the result
    val pendingIter = pendingActions.entrySet().iterator()
    while (pendingIter.hasNext) {
      val entry = pendingIter.next()
      val key = entry.getKey
      if (predicate(key)) {
        val value = entry.getValue
        map.compute(key, (_, v) => {
          if (v eq null) value.storeObject.toStats else v.addWeightage(value.addedWeightage)
        })
      }
    }

    map.asScala
  }

  override def putStatistics(statistics: StatisticsMap): Unit = runBlocking {
    statistics.foreach { e =>
      val k = e._1
      if (!pendingActions.containsKey(k) && !cacheMap.containsKey(k)) {
        evictedEntries.put(k, e._2)
      }
    }
  }

  override def removeObject(key: Comparable[_ <: AnyRef]): Boolean = runBlocking {
    var removed = false
    val cached = cacheMap.remove(key)
    if (cached ne null) {
      assert(evictionSet.remove(cached))
      release(cached, isEvicted = false)
      removed = true
    }
    val pending = pendingActions.remove(key)
    if ((pending ne null) && (cached eq null)) {
      pending.abort()
      removed = true
    }
    evictedEntries.remove(key)
    removed
  }

  override def removeAll(predicate: Comparable[_ <: AnyRef] => Boolean): Int = runBlocking {
    var numRemoved = 0
    // iterate pendingActions and remove all matching entries
    val pendingIter = pendingActions.entrySet().iterator()
    while (pendingIter.hasNext) {
      val entry = pendingIter.next()
      val key = entry.getKey
      if (predicate(key)) {
        val value = entry.getValue
        pendingIter.remove()
        // remove from cache if present
        val cached = cacheMap.remove(key)
        if (cached ne null) {
          assert(evictionSet.remove(cached))
          release(cached, isEvicted = false)
        }
        if (cached ne value.storeObject) value.abort()
        numRemoved += 1
      }
    }
    // next all cached entries (those in pending list have already been removed above)
    val cachedIter = cacheMap.entrySet().iterator()
    while (cachedIter.hasNext) {
      val entry = cachedIter.next()
      val key = entry.getKey
      if (predicate(key)) {
        val cached = entry.getValue
        cachedIter.remove()
        assert(evictionSet.remove(cached))
        release(cached, isEvicted = false)
        numRemoved += 1
      }
    }
    // cleanup evictedEntries statistics at the end but these are not added to the result count
    evictedEntries.removeAll((k, _) => predicate(k))

    numRemoved
  }

  override def checkAndForceConsistency(): Seq[String] = {
    def checkAndForce(): Seq[String] = {
      val errors = new ArrayBuffer[String](4)
      val evictionIter = evictionSet.iterator()
      while (evictionIter.hasNext) {
        val storedObject = evictionIter.next()
        val key = storedObject.key
        val value = storedObject.value
        if ((value eq null) || !value.isInUse) {
          cacheMap.remove(key)
          evictionIter.remove()
          val valStr = if (value eq null) "null" else "released"
          val error = s"consistency check: found $valStr value in evictionSet for $key"
          errors += error
          logError(error)
        } else {
          val cacheMapValue = cacheMap.get(key)
          if (storedObject ne cacheMapValue) {
            cacheMap.remove(key)
            evictionIter.remove()
            val error = s"consistency check: inconsistent value in the two maps for $key:: " +
              s"evictionSet value = [$storedObject] cacheMap value = [$cacheMapValue]"
            errors += error
            logError(error)
          } else {
            // valid entry
            if (evictedEntries.remove(key) ne null) {
              val error = s"consistency check: found cached key in evictedEntries for $key"
              errors += error
              logError(error)
            }
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
        val storedKey = storedObject.key
        if ((storedValue eq null) || key != storedKey) {
          cacheIter.remove()
          evictionSet.remove(storedObject)
          val valStr =
            if (storedValue eq null) "null"
            else s"different keys [StoredCacheObject key = $storedKey] for"
          val error = s"consistency check: found $valStr value in cacheMap for $key"
          errors += error
          logError(error)
        } else if (!evictionSet.contains(storedObject)) {
          cacheIter.remove()
          val error = s"consistency check: missing cached value in evictionSet for $key"
          errors += error
          logError(error)
        } else {
          // valid entry
          used += storedValue.memorySize
        }
      }
      val currentUsed = usedMemory.get()
      if (used != currentUsed) {
        usedMemory.set(used)
        val error = s"consistency check: usedMemory = $currentUsed, calculated = $used"
        errors += error
        logError(error)
      }

      if (evictedEntries.shrinkToMaxCapacity()) {
        val error = s"consistency check: evicted entries over the limit of $maxEvictedEntries"
        errors += error
        logError(error)
      }

      assert(cacheMap.size() == evictionSet.size())

      errors
    }
    runBlocking(checkAndForce(), maxMemoryWriteLocked = true)
  }

  /**
   * Convenience method to either perform caching related work followed by eviction, if required,
   * or else push the work item into [[pendingActions]].
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock]] or [[maxMemoryLock.writeLock]]
   *       has been acquired before invoking this method.
   */
  private def cacheAndEvict[T <: CacheValue](
      key: Comparable[_ <: AnyRef],
      pendingAction: PendingAction[T],
      timeWeightage: Double): Unit = {
    // push pendingAction in the pending map with read lock to ensure any concurrent runEviction
    // flushes the map completely before this one is put that may change existing pendingAction
    pendingActionLock.readLock().lockInterruptibly()
    try {
      pendingActions.merge(key, pendingAction, (_, v) => pendingAction.merge(v))
    } finally {
      pendingActionLock.readLock().unlock()
    }
    if (runEviction(blocking = false, timeWeightage) == Long.MinValue) {
      // check if usedMemory is over maxMemory by more than maxOverflowPercent percent
      if (pendingAction.addedMemorySize > 0L &&
          usedMemory.get().toDouble >= (1.0 + maxOverflowPercent) * maxMemory.toDouble) {
        runEviction(blocking = true, timeWeightage)
      }
    }
  }

  /**
   * Run the eviction loop in order to bring [[usedMemory]] below [[maxMemory]] and clear up any
   * work items in [[pendingActions]]. Only one thread can perform eviction at a time and
   * in order to minimize the bottleneck of multiple threads waiting for [[runEviction]], the
   * other threads should queue up their work in [[pendingActions]] as long as [[usedMemory]]
   * has not exceeded [[maxMemory]] by more than [[maxOverflowPercent]] (for the latter
   * case the thread will need to either wait for [[runEviction]] to complete or skip caching).
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock]] or [[maxMemoryLock.writeLock]]
   *       has been acquired before invoking this method.
   *
   * @param blocking when true then [[evictionLock]] is acquired in a blocking manner else a
   *                 `tryLock` with zero timeout is attempted
   * @param timeWeightage the current timestamp which should be the [[System.currentTimeMillis]]
   *                  at the start of operation
   * @param maxFlushPending skip and block flushPendingActions after these many iterations
   *
   * @return if [[evictionLock.tryLock]] was acquired then the number of bytes evicted else
   *         [[Long.MinValue]] if eviction was skipped (in which case the caller is supposed
   *         to queue the [[PendingAction]] in [[pendingActions]] map if the total used memory
   *         has not exceeded [[maxMemory]] by more than [[maxOverflowPercent]])
   */
  private def runEviction[T <: CacheValue](
      blocking: Boolean,
      timeWeightage: Double,
      maxFlushPending: Int = 20): Long = {

    val pendingCompressedObjects = new ArrayBuffer[CompressedCacheObject[C, D]](4)
    var remainingFlushes = maxFlushPending
    val max = maxMemory

    if (blocking) evictionLock.lockInterruptibly()
    else if (!evictionLock.tryLock()) {
      return Long.MinValue // indicates that runEviction was skipped
    }
    try {
      flushPendingActions()
      remainingFlushes -= 1
      if (max >= usedMemory.get()) return 0L

      var evictedSize = 0L
      var removed = evictionSet.pollFirst()
      while (removed ne null) {
        if (remainingFlushes >= 0 && flushPendingActions()) {
          remainingFlushes -= 1
        }
        var evicted = true
        if (removed.generation > 0 && !removed.hasGenerationalBoost) {
          if (!currentFlushedEntries.contains(removed)) {
            val flushedIter = currentFlushedEntries.iterator()
            while (evicted && flushedIter.hasNext) {
              val flushedObject = flushedIter.next()
              // if a object having higher generation is evicting "removed" (i.e. was pushed
              // out of cache more) and its weightage ignoring that due to current scan is lower
              // then there is a high likelihood that "removed" will also be faulted in by the
              // same scan (that faulted in "flushedObject") or some other soon, hence add a
              // boost to the weightage of the "removed" and find new candidates for eviction
              if (flushedObject.generation > removed.generation &&
                  flushedObject.weightage < removed.weightage + timeWeightage) {
                // this is the case where it is anticipated that removed may be faulted back
                // into the cache quickly so temporarily apply a boost and record it
                removed.addGenerationalBoost(timeWeightage)
                evictionSet.add(removed)
                evicted = false
              }
            }
          }
        }
        if (evicted) {
          try {
            assert(cacheMap.remove(removed.key) eq removed)
            val removedVal = removed.value
            var removedSize = removedVal.memorySize
            val transformer =
              removed.transformer.asInstanceOf[TransformValue[CacheValue, CacheValue]]
            // for decompressed blocks, compress and put them back rather than evicting entirely
            if (!removed.isCompressed && transformer.compressionAlgorithm.isDefined &&
                smallestWeightage() < removed.weightage + Utils.calcCompressionSavings(
                  timeWeightage,
                  decompressionToDiskReadCost,
                  transformer.compressedSize(removedVal),
                  removedSize)) {
              val compressed = removed
                .asInstanceOf[DecompressedCacheObject[D, C]]
                .compress(removedVal.asInstanceOf[D], timeWeightage, decompressionToDiskReadCost)
              val compressedVal = compressed.value
              val compressedSize = compressedVal.memorySize
              if (compressedSize < removedSize && compressed.weightage > removed.weightage) {
                compressedVal.use() // value can be put in cache so mark as in-use
                usedMemory.addAndGet(compressedSize)
                pendingCompressedObjects += compressed
                removedSize -= compressedSize
                // skip evictedEntries in the release() call in the finally block below
                evicted = false
              } else compressedVal.finalizeSelf()
            }
            evictedSize += removedSize
          } finally {
            // if compressed value is to be cached then skip entry in evictedEntries
            release(removed, evicted)
          }
        }
        // break the loop if usedMemory has fallen below maxMemory
        if (max >= usedMemory.get()) return evictedSize

        removed = evictionSet.pollFirst()
      }
      // insert the pending compressed objects and try again
      if (flushPendingCompressedObjects(pendingCompressedObjects)) {
        evictedSize + runEviction(blocking = true, timeWeightage, remainingFlushes)
      } else evictedSize
    } finally {
      try {
        flushPendingCompressedObjects(pendingCompressedObjects)
        currentFlushedEntries.clear()
      } finally {
        evictionLock.unlock()
      }
    }
  }

  private def runIgnoreException(work: () => Boolean): Boolean = {
    try {
      if (work()) {
        work match {
          case p: PendingAction[_] =>
            assert(evictionLock.isHeldByCurrentThread)
            currentFlushedEntries.add(p.storeObject)
          case _ =>
        }
        true
      } else false
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
        false
    }
  }

  /*
  private def isNonFatalException(t: Throwable): Boolean = {
    !t.isInstanceOf[OutOfMemoryError] || t.isInstanceOf[SparkOutOfMemoryError] ||
    t.getMessage.contains("Direct buffer") // direct buffer allocation failures are non-fatal
  } */

  @GuardedBy("evictionLock")
  private def putIntoCache[T <: CacheValue](storeObject: StoredCacheObject[T]): Boolean = {
    var success = false
    try {
      val oldCached = cacheMap.put(storeObject.key, storeObject)
      // if a thread gets hold of the object in cacheMap, then it will use UpdateWeightageForAccess
      assert(oldCached ne storeObject)
      if (oldCached ne null) {
        assert(evictionSet.remove(oldCached))
        release(oldCached, isEvicted = false)
      }
      assert(evictionSet.add(storeObject))
      evictedEntries.remove(storeObject.key)
      success = true
      true
    } finally {
      if (!success) release(storeObject, isEvicted = false)
    }
  }

  @GuardedBy("evictionLock")
  private def updateWeightage(
      storeObject: StoredCacheObject[_ <: CacheValue],
      addedWeightage: Double): Boolean = {
    // remove the additional boost provided before the block was scanned and since this
    // is run by a single thread so there is no race condition in reading+writing
    val delta = addedWeightage + storeObject.weightageWithoutBoost - storeObject.weightage
    if (delta != 0.0 && evictionSet.remove(storeObject)) {
      storeObject.weightage += delta
      assert(evictionSet.add(storeObject))
      true
    } else {
      // update statistics for the key if present
      if (addedWeightage != 0.0) {
        evictedEntries.update(storeObject.key, (_, v) => v.addWeightage(addedWeightage))
      }
      false
    }
  }

  @GuardedBy("evictionLock")
  private def flushPendingActions(): Boolean = {
    var actions = Array.emptyObjectArray
    pendingActionLock.writeLock().lockInterruptibly()
    try {
      if (!pendingActions.isEmpty) {
        actions = pendingActions.values().toArray
        pendingActions.clear()
      }
    } finally {
      pendingActionLock.writeLock().unlock()
    }
    if (actions.length > 0) {
      for (action <- actions) runIgnoreException(action.asInstanceOf[PendingAction[_]])
      true
    } else false
  }

  @GuardedBy("evictionLock")
  private def flushPendingCompressedObjects(
      objects: ArrayBuffer[CompressedCacheObject[C, D]]): Boolean = {
    val numObjects = objects.length
    if (numObjects != 0) {
      var i = 0
      while (i < numObjects) {
        val p = objects(i)
        i += 1
        runIgnoreException(() => putIntoCache(p))
      }
      objects.clear()
      true
    } else false
  }

  /**
   * Base class for pending actions put in [[pendingActions]] that are evaluated by the thread
   * that executes [[runEviction]]. Any changes to [[cacheMap]], [[evictionSet]] or
   * [[evictedEntries]] should be done by a single thread that executes [[runEviction]] either
   * in a blocking manner (only done by remove operations) or non-blocking way by putting the
   * action into the [[pendingActions]] using the [[cacheAndEvict]] method.
   *
   * @tparam T the concrete type of [[CacheValue]] contained in [[storeObject]]
   */
  sealed abstract class PendingAction[T <: CacheValue] extends (() => Boolean) {

    /**
     * The [[StoredCacheObject]] effected by this action.
     */
    def storeObject: StoredCacheObject[T]

    /**
     * New size added to [[usedMemory]] for this action (done before this action is executed
     * since [[PendingAction]] itself will take up that memory).
     */
    def addedMemorySize: Long

    /**
     * Extra `weightage` to be added to [[storeObject]] as a result of execution of this action.
     */
    def addedWeightage: Double

    /**
     * The result of merging two [[PendingAction]]s for the same `key`.
     */
    def merge[U <: CacheValue](other: PendingAction[U]): PendingAction[_ <: CacheValue]

    /**
     * Abort the action reverting any changes already done.
     */
    def abort(): Unit
  }

  private final class CachePending[T <: CacheValue](
      override val storeObject: StoredCacheObject[T],
      private val timeWeightage: Double)
      extends PendingAction[T] {

    override def addedMemorySize: Long = storeObject.value.memorySize

    override def addedWeightage: Double = 0.0

    @GuardedBy("evictionLock")
    override def apply(): Boolean = putIntoCache(storeObject)

    override def merge[U <: CacheValue](
        other: PendingAction[U]): PendingAction[_ <: CacheValue] = {
      other match {
        case null => this
        case c: CachePending[_] =>
          // storeObjects in CachePending are not in cacheMap so they cannot be the same
          assert(storeObject ne c.storeObject)
          storeObject.weightage += c.timeWeightage
          c.abort()
          this
        case u: UpdateWeightageForAccess[_] =>
          // storeObject cannot have a boost since it is not in the cache yet
          storeObject.weightage += u.addedWeightage
          this
      }
    }

    override def abort(): Unit = release(storeObject, isEvicted = false)
  }

  private final class UpdateWeightageForAccess[T <: CacheValue](
      override val storeObject: StoredCacheObject[T],
      private[this] var _addedWeightage: Double)
      extends PendingAction[T] {

    override def addedMemorySize: Long = 0L

    override def addedWeightage: Double = _addedWeightage

    @GuardedBy("evictionLock")
    override def apply(): Boolean = updateWeightage(storeObject, _addedWeightage)

    override def merge[U <: CacheValue](
        other: PendingAction[U]): PendingAction[_ <: CacheValue] = {
      other match {
        case null => this
        case c: CachePending[_] => c.merge(this)
        case u: UpdateWeightageForAccess[_] =>
          _addedWeightage += u.addedWeightage
          this
      }
    }

    override def abort(): Unit = {}
  }
}

/**
 * A holder for the various fields related to weightage adjustment for current time so that
 * all those are replaced atomically by changing the object reference.
 *
 * @param millisForTwiceFreq milliseconds after which the timestamp will have same weightage
 *                           as two accesses as of now; this should not be smaller than 10 secs
 *                           and recommended to be 5 minutes or more else the exponent required
 *                           for weightage becomes too large
 * @param timestamp time in millis for which `epoch` and `exponent` were calculated
 */
private final class CurrentTimeAdjustment(
    private[this] val millisForTwiceFreq: Long,
    private[this] val timestamp: Long) {

  /**
   * Milliseconds after which [[exponent]] is recalculated which is fixed at 1 hour.
   */
  private[this] val refreshMillis: Long = 60L * 60L * 1000L

  /**
   * Time is measured from this point onwards and converted to double by `timeToDouble`.
   */
  private[this] val epoch: Long = timestamp - refreshMillis

  /**
   * This is the exponent used to blow up current time contribution to `weightage` so that
   * multiple past accesses do not have higher priority over recent single access.
   *
   * The way this is evaluated is to use the passed [[millisForTwiceFreq]] to determine the time
   * after which two accesses in the past are equivalent to one current access. So this is
   * the `X` in the equation: `currentTime ^ X = 2 * (currentTime - millisForTwiceFreq) ^ X`.
   *
   * Since System.currentTimeMillis is quite large and results in an unreasonably large exponent
   * using the above equation, the `epoch` time used is 1 hour behind current time which works
   * nicely with refresh time being the same interval of 1 hour.
   */
  private[this] val exponent: Double = calcCurrentTimeAdjustment(timestamp)

  require(
    exponent <= 500.0,
    s"Exponent calculated for millisForTwiceFreq = $millisForTwiceFreq is too large ($exponent)." +
      s" Recommended millisForTwiceFreq is 5 minutes or more and no less than 10 secs.")

  /**
   * Convert timestamp longs to a reasonable double value.
   */
  @inline
  private def timeToDouble(millis: Long): Double = (millis - epoch).toDouble / 3000000.0

  private def calcCurrentTimeAdjustment(millis: Long): Double = {
    val twiceFreqTime = millis - millisForTwiceFreq
    math.log(2.0) / (math.log(timeToDouble(millis)) - math.log(timeToDouble(twiceFreqTime)))
  }

  /**
   * If [[timestamp]] is older than given time by more than [[refreshMillis]].
   */
  @inline
  def needsRefresh(timeInMillis: Long): Boolean = {
    timeInMillis - timestamp > refreshMillis
  }

  /**
   * Adjust the `weightage` of current time by raising it to power of [[exponent]].
   */
  @inline
  def millisTimeAdjusted(timeInMillis: Long): Double = {
    math.pow(timeToDouble(timeInMillis), exponent)
  }
}
