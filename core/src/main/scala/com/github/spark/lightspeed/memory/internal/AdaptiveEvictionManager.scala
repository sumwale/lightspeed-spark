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
    maxMemoryOverflowBeforeEvict > 0.0 && maxMemoryOverflowBeforeEvict < 0.5,
    "maximum ratio that can overflow before blocking for eviction should be (0.0, 0.5) " +
      s"but was $maxMemoryOverflowBeforeEvict")
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
   *
   * NOTE: This map is supposed to be accessed and updated only by the eviction thread hence
   *       is neither concurrent nor thread-safe.
   */
  @GuardedBy("evictionLock")
  private[this] val evictedEntries =
    Collections.newLRUHashMap[Comparable[_ <: AnyRef], CacheValueStats]()

  /**
   * Map to hold pending caching work. Callers should ensure that [[usedMemory]] should not exceed
   * [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] before pushing new work items
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
   * Adjust the `weightage` of current time using [[CurrentTimeAdjustment.millisTimeAdjusted]].
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
   * Release a [[CacheValue]] explicitly using the [[FinalizeValue]] returned by
   * [[TransformValue.createFinalizer]].
   */
  private def releaseValue(
      value: CacheValue,
      transformer: TransformValue[_ <: CacheValue, _ <: CacheValue]): Unit = {
    transformer.createFinalizer(value) match {
      case Some(f) =>
        // if finalizer field is null, no entry was made in EvictionService's map (unless done
        // outside of EvictionManager explicitly which will be managed outside of this)
        f.clear(value.finalizer eq null)
        f.finalizeReferent()
      case _ =>
    }
  }

  /**
   * Release a value that was in the cache that is assumed to be removed from [[cacheMap]] and
   * [[evictionSet]] prior to this method being invoked. This does [[usedMemory]] bookkeeping,
   * [[FinalizeValue]] handling of the value and adds to [[evictedEntries]].
   *
   * @param cached the [[StoredCacheObject]] removed from the cache
   * @param markEvicted if true then add statistics to [[evictedEntries]] else skip it
   */
  private def release(
      cached: StoredCacheObject[_ <: CacheValue],
      markEvicted: Boolean = true): Unit = {
    val value = cached.value
    usedMemory.addAndGet(-value.memorySize)
    val transformer = cached.transformer
    // if value has no finalizer field prior to release() then it needs to be finalized explicitly
    val finalizer = value.finalizer
    if (value.release()) {
      // apply finalization without looking up the WeakReference map in EvictionService
      // since no entry would have been made there if finalizer field was null
      if (finalizer eq null) releaseValue(value, transformer)
    } else {
      // other references exist so add finalizer to CacheValue and EvictionService's map
      addFinalizerIfMissing(value, transformer)
    }
    // move to evictedEntries map (except for remove operations) and shrink the map if too large
    if (markEvicted) {
      assert(evictionLock.isHeldByCurrentThread)
      evictedEntries.put(cached.key, cached.toStats)
      shrinkEvictedEntriesIfRequired()
    }
  }

  /**
   * If [[evictedEntries]] size has exceeded the provided [[maxEvictedEntries]] parameter, then
   * remove as many oldest updated entries as required to reach that limit.
   */
  @GuardedBy("evictionLock")
  private def shrinkEvictedEntriesIfRequired(): Boolean = {
    if (evictedEntries.size() > maxEvictedEntries) {
      val iter = evictedEntries.values().iterator()
      while (iter.hasNext) {
        iter.remove()
        if (evictedEntries.size() <= maxEvictedEntries) return true
      }
      true
    } else false
  }

  override def setLimit(newMaxMemory: Long, timestamp: Long): Unit = {
    maxMemoryLock.writeLock().lockInterruptibly()
    try {
      maxMemory = newMaxMemory
      runEviction(blocking = true, pendingAction = null, millisTimeAdjusted(timestamp))
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putObject(
      key: Comparable[_ <: AnyRef],
      either: Either[C, D],
      transformer: TransformValue[C, D],
      timestamp: Long): Unit = {

    cacheObject(key, either, transformer, timestamp, fromPut = true)
  }

  /**
   * Like [[putObject]] but adds a parameter `fromPut` that will skip throwing
   * [[UnsupportedOperationException]] if it is false and value size is greater than total memory.
   * It will also immediately release on-the-fly value that does not get cached. This method is
   * used to cache an object by [[putObject]] and by [[getDecompressed]] for cache miss.
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
      existing: CompressedCacheObject[C, D] = null): Boolean = {

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

      if (maxMemorySize > maxMemory) {
        throw new UnsupportedOperationException(
          s"Cannot put object of $memorySize bytes with maxMemory = $maxMemory")
      }
      val timeWeightage = millisTimeAdjusted(timestamp)
      val cached = Utils.newStoredCacheObject(
        key,
        value,
        transformer,
        timeWeightage,
        decompressionToDiskReadCost)
      if (!fromPut) {
        // for values from loader or intermediate results, increase referenceCount since this
        // will be sent outside to caller and so should have one referenceCount incremented
        // as per the contract of getDecompressed()
        value.use()
        if (existing ne null) {
          cached.weightage += (existing.weightageWithoutBoost - existing.compressionSavings)
          // quick check to determine if the object being inserted has a lower weightage than
          // the smallest one in the evictionSet which is fine for this case since the key cannot
          // be in evictedEntries so the weightage so far is accurate; in the worst case this can
          // still end up putting decompressed object in cache while runEviction can remove it or
          // change to compressed form again but that should be very rare
          if (smallestWeightage() < cached.weightage) {
            throw new UnsupportedOperationException("Cannot cache due to small 'weightage'")
          }
        }
      }
      value.use() // value can be put in cache so mark as in-use
      usedMemory.addAndGet(memorySize)
      cachingStarted = true
      cacheAndEvict(key, new CachePending(cached, timeWeightage, existing), timeWeightage)
      true
    } catch {
      case t: Throwable =>
        if (cachingStarted) {
          // revert the changes done before cacheAndEvict failed
          usedMemory.addAndGet(-value.memorySize)
          // normal release() is done after adding finalizer field below
          // release() for !fromPut not required since that will invoke releaseValue() below
        }
        if (fromPut || t.isInstanceOf[UnsupportedOperationException]) {
          // add finalizer for cleanup of value
          addFinalizerIfMissing(value, transformer)
          if (cachingStarted) value.release()
          if (fromPut) throw t else false
        } else {
          // finalize value immediately
          releaseValue(value, transformer)
          throw t
        }
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  /**
   * Set the `finalizer` field of [[CacheValue]] if empty using [[TransformValue.createFinalizer]]
   * and if created, put in [[EvictionService]]'s map to maintain the `WeakReference`.
   */
  private def addFinalizerIfMissing(
      value: CacheValue,
      transformer: TransformValue[_ <: CacheValue, _ <: CacheValue]): Unit = {
    if (value.finalizer eq null) {
      transformer.createFinalizer(value) match {
        case Some(f) =>
          value.finalizer = f
          // maintain in a separate map for pending WeakReferences that will be cleared when the
          // underlying CacheValue is finalized
          val finalizer = value.finalizer
          if (finalizer ne null) EvictionService.addWeakReference(finalizer)
        case _ =>
      }
    }
  }

  /**
   * Update the `weightage` of an existing [[StoredCacheObject]] for read using current timestamp.
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
        touchObject(key, d, timestamp)
        Some(cachedValue.asInstanceOf[D])

      case c: CompressedCacheObject[C, D] =>
        val transformer = c.transformer.asInstanceOf[TransformValue[C, D]]
        val decompressedValue = try {
          transformer.decompress(cachedValue.asInstanceOf[C])
        } finally {
          cachedValue.release() // release the extra reference count added by tryUse() before
        }
        val either = Right[C, D](decompressedValue)
        cacheObject(key, either, transformer, timestamp, fromPut = false, c)
        Some(decompressedValue)
    } else {
      loader match {
        case Some(l) =>
          l(key) match {
            case Some((either, transformer)) =>
              // return the loaded object decompressing, if required, and caching the result
              either match {
                case Left(value) =>
                  try {
                    val decompressed = transformer.decompress(value)
                    val right = Right[C, D](decompressed)
                    cacheObject(key, right, transformer, timestamp, fromPut = false)
                    Some(decompressed)
                  } finally {
                    releaseValue(value, transformer)
                  }
                case Right(value) =>
                  cacheObject(key, either, transformer, timestamp, fromPut = false)
                  Some(value)
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
   * also start blocking since they acquire the [[maxMemoryLock.readLock]].
   */
  private def runBlocking[T](action: => T, maxMemoryWriteLocked: Boolean = false): T = {
    var unlockEviction = false
    val mLock = if (maxMemoryWriteLocked) maxMemoryLock.writeLock() else maxMemoryLock.readLock()
    mLock.lockInterruptibly()
    try {
      evictionLock.lockInterruptibly()
      unlockEviction = true

      action

    } finally {
      if (unlockEviction) evictionLock.unlock()
      mLock.unlock()
    }
  }

  override def getStatistics(predicate: Comparable[_ <: AnyRef] => Boolean)
    : scala.collection.Map[Comparable[_ <: AnyRef], CacheValueStats] = {
    val map = Collections.newOpenHashMap[Comparable[_ <: AnyRef], CacheValueStats]

    // evictionLock is only acquired when iterating through evictedEntries to minimize contention

    // iterate evictedEntries first since those are lowest priority and can be overwritten later
    runBlocking {
      val evictedIter = evictedEntries.entrySet().iterator()
      while (evictedIter.hasNext) {
        val entry = evictedIter.next()
        val key = entry.getKey
        if (predicate(key)) map.put(key, entry.getValue)
      }
    }
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
          if (v eq null) value.storeObject.toStats else v.addWeightage(value.objectWeightage)
        })
      }
    }

    map.asScala
  }

  override def putStatistics(
      statistics: scala.collection.Map[Comparable[_ <: AnyRef], CacheValueStats]): Unit = {
    runBlocking {
      statistics.foreach { e =>
        val k = e._1
        if (!pendingActions.containsKey(k) && !cacheMap.containsKey(k)) {
          evictedEntries.put(k, e._2)
        }
      }
    }
  }

  override def removeObject(key: Comparable[_ <: AnyRef]): Boolean = runBlocking {
    var removed = false
    val cached = cacheMap.remove(key)
    if (cached ne null) {
      assert(evictionSet.remove(cached))
      release(cached, markEvicted = false)
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
        // remove from cache if present and add pendingAction's extra weightage
        val cached = cacheMap.remove(key)
        if (cached ne null) {
          assert(evictionSet.remove(cached))
          release(cached, markEvicted = false)
        } else {
          value.abort()
        }
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
        release(cached, markEvicted = false)
        numRemoved += 1
      }
    }
    // cleanup evictedEntries statistics at the end but these are not added to the result count
    val evictedIter = evictedEntries.entrySet().iterator()
    while (evictedIter.hasNext) {
      val entry = evictedIter.next()
      val key = entry.getKey
      if (predicate(key)) evictedIter.remove()
    }
    numRemoved
  }

  override def checkAndForceConsistency(): Boolean = {
    def checkAndForce(): Boolean = {
      var consistent = true
      val evictionIter = evictionSet.iterator()
      while (evictionIter.hasNext) {
        val storedObject = evictionIter.next()
        val key = storedObject.key
        val value = storedObject.value
        if ((value eq null) || !value.isInUse) {
          cacheMap.remove(key)
          evictionIter.remove()
          consistent = false
          val valStr = if (value eq null) "null" else "released"
          logError(s"consistency check: found $valStr value in evictionSet for $key")
        } else {
          val cacheMapValue = cacheMap.get(key)
          if (storedObject ne cacheMapValue) {
            cacheMap.remove(key)
            evictionIter.remove()
            consistent = false
            logError(
              s"consistency check: inconsistent value in the two maps for $key:: " +
                s"evictionSet value = [$storedObject] cacheMap value = [$cacheMapValue]")
          } else {
            // valid entry
            if (evictedEntries.remove(key) ne null) {
              consistent = false
              logError(s"consistency check: found evictionSet key in evictedEntries for $key")
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
          consistent = false
          val valStr =
            if (storedValue eq null) "null"
            else s"different keys [StoredCacheObject key = $storedKey] for"
          logError(s"consistency check: found $valStr value in cacheMap for $key")
        } else if (!evictionSet.contains(storedObject)) {
          cacheIter.remove()
          consistent = false
          logError(s"consistency check: missing cacheMap value in evictionSet for $key")
        } else {
          // valid entry
          used += storedValue.memorySize
        }
      }
      val currentUsed = usedMemory.get()
      if (used != currentUsed) {
        usedMemory.set(used)
        consistent = false
        logError(s"consistency check: usedMemory = $currentUsed, calculated = $used")
      }

      if (shrinkEvictedEntriesIfRequired()) {
        consistent = false
        logError(s"consistency check: evicted entries over limit of $maxEvictedEntries")
      }
      if (evictedEntries.size() > maxEvictedEntries) {
        System.gc()
        if (shrinkEvictedEntriesIfRequired()) {
          consistent = false
          logError(s"consistency check: evicted entries still over limit of $maxEvictedEntries")
        }
      }

      assert(cacheMap.size() == evictionSet.size())

      consistent
    }
    runBlocking(checkAndForce(), maxMemoryWriteLocked = true)
  }

  /**
   * Convenience method to either perform caching related work followed by eviction, if required,
   * or else push the work item into [[pendingActions]].
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock]] has been acquired
   *       before invoking this method.
   */
  private def cacheAndEvict[T <: CacheValue](
      key: Comparable[_ <: AnyRef],
      pendingAction: PendingAction[T],
      timeWeightage: Double): Unit = {
    if (runEviction(blocking = false, pendingAction, timeWeightage) == Long.MinValue) {
      // check if usedMemory is over maxMemory by too much
      val used = usedMemory.get()
      if (used.toDouble >= (1.0 + maxMemoryOverflowBeforeEvict) * maxMemory.toDouble) {
        runEviction(blocking = true, pendingAction, timeWeightage)
      } else {
        enqueuePendingAction(key, pendingAction)
      }
    }
  }

  /**
   * Put a [[PendingAction]] into the [[pendingActions]] merging with existing value for the same
   * key, if any.
   */
  private def enqueuePendingAction[T <: CacheValue](
      key: Comparable[_ <: AnyRef],
      pendingAction: PendingAction[T]): Unit = {
    pendingActionLock.readLock().lockInterruptibly()
    try {
      pendingActions.merge(key, pendingAction, (_, v) => pendingAction.merge(v))
    } finally {
      pendingActionLock.readLock().unlock()
    }
  }

  /**
   * Run the eviction loop in order to bring [[usedMemory]] below [[maxMemory]] and clear up any
   * work items in [[pendingActions]]. Only one thread can perform eviction at a time and
   * in order to minimize the bottleneck of multiple threads waiting for [[runEviction]], the
   * other threads should queue up their work in [[pendingActions]] as long as [[usedMemory]]
   * has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] (for the latter
   * case the thread will need to either wait for [[runEviction]] to complete or skip caching).
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock]] or [[maxMemoryLock.writeLock]]
   *       has been acquired before invoking this method.
   *
   * @param blocking when true then [[evictionLock]] is acquired in a blocking manner else a
   *                 `tryLock` with zero timeout is attempted
   * @param pendingAction the work required to be done by current thread if [[evictionLock]] was
   *                     acquired, else caller is supposed to queue it in [[pendingActions]]
   *                     and let the other thread that successfully acquires it execute the method
   * @param timeWeightage the current timestamp which should be the [[System.currentTimeMillis]]
   *                  at the start of operation
   * @param maxFlushPending skip and block flushPendingActions after these many iterations
   *
   * @return if [[evictionLock.tryLock]] was acquired then the number of bytes evicted else
   *         [[Long.MinValue]] if eviction was skipped (in which case the caller is supposed
   *         to queue the [[PendingAction]] in [[pendingActions]] map if the total used memory
   *         has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]])
   */
  private def runEviction[T <: CacheValue](
      blocking: Boolean,
      pendingAction: PendingAction[T],
      timeWeightage: Double,
      maxFlushPending: Int = 20): Long = {
    if (blocking) evictionLock.lockInterruptibly()
    else if (!evictionLock.tryLock()) {
      return Long.MinValue // indicates that runEviction was skipped
    }
    val pendingCompressedObjects = new java.util.ArrayList[CompressedCacheObject[C, D]]()
    var remainingFlushes = maxFlushPending
    val max = maxMemory

    try {
      flushPendingActions(remainingFlushes == 0)
      remainingFlushes -= 1
      if (pendingAction ne null) runIgnoreException(pendingAction)
      if (max >= usedMemory.get()) {
        flushPendingActions(remainingFlushes == 0)
        remainingFlushes -= 1
        return 0L
      }

      var evicted = 0L
      var evictionIter = evictionSet.iterator()
      while (evictionIter.hasNext) {
        val candidate = evictionIter.next()
        val removeKey = candidate.key
        val removedVal = candidate.value
        assert(removedVal ne null)
        if (remainingFlushes >= 0 && flushPendingActions(remainingFlushes == 0)) {
          remainingFlushes -= 1
          // evictionSet changed so restart the iterator
          evictionIter = evictionSet.iterator()
        } else {
          var continueRemoval = true
          if (candidate.generation > 0) {
            if (!currentFlushedEntries.contains(candidate)) {
              val flushedIter = currentFlushedEntries.iterator()
              while (continueRemoval && flushedIter.hasNext) {
                val flushedObject = flushedIter.next()
                // if a object having higher generation is evicting "candidate" (i.e. was pushed
                // out of cache more) and its weightage ignoring that due to current scan is lower
                // then there is a high likelihood that "candidate" will also be faulted in by the
                // same scan (that faulted in "flushedObject") or some other soon, hence add a
                // boost to the weightage of the "candidate" and find new candidates for eviction
                if (flushedObject.generation > candidate.generation &&
                    flushedObject.weightage < candidate.weightage + timeWeightage) {
                  // this is the case where it is anticipated that candidate may be faulted back
                  // into the cache quickly so temporarily apply a boost and record it
                  updateWeightage(candidate, timeWeightage, isBoost = true)
                  evictionIter = evictionSet.iterator()
                  continueRemoval = false
                }
              }
            }
          }
          if (continueRemoval) {
            assert(cacheMap.remove(removeKey) eq candidate)
            evictionIter.remove()
            var removedSize = removedVal.memorySize
            try {
              val transformer =
                candidate.transformer.asInstanceOf[TransformValue[CacheValue, CacheValue]]
              // for decompressed blocks, compress and put them back rather than evicting entirely
              if (!candidate.isCompressed && transformer.compressionAlgorithm.isDefined &&
                  smallestWeightage() < candidate.weightage + Utils.calcCompressionSavings(
                    timeWeightage,
                    decompressionToDiskReadCost,
                    transformer.compressedSize(removedVal),
                    removedSize)) {
                val compressed = candidate
                  .asInstanceOf[DecompressedCacheObject[D, C]]
                  .compress(
                    removedVal.asInstanceOf[D],
                    timeWeightage,
                    decompressionToDiskReadCost)
                val compressedVal = compressed.value
                val compressedSize = compressedVal.memorySize
                if (compressed.weightage > candidate.weightage) {
                  compressedVal.use() // value can be put in cache so mark as in-use
                  usedMemory.addAndGet(compressedSize)
                  pendingCompressedObjects.add(compressed)
                  removedSize -= compressedSize
                } else if (compressedVal.finalizer eq null) {
                  releaseValue(compressedVal, compressed.transformer)
                }
              }
            } finally {
              release(candidate)
            }
            evicted += removedSize
          }
          // break the loop if usedMemory has fallen below maxMemory
          if (max >= usedMemory.get()) return evicted
        }
      }
      // insert the pending compressed objects and try again
      if (flushPendingCompressedObjects(pendingCompressedObjects)) {
        evicted + runEviction(
          blocking = true,
          pendingAction = null,
          timeWeightage,
          remainingFlushes)
      } else evicted
    } finally {
      try {
        flushPendingCompressedObjects(pendingCompressedObjects)
      } finally {
        currentFlushedEntries.clear()
        evictionLock.unlock()
        if (remainingFlushes < 0 && maxFlushPending >= 0) {
          pendingActionLock.writeLock().unlock()
        }
      }
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
    val oldCached = cacheMap.put(storeObject.key, storeObject)
    // if a thread gets hold of the object in cacheMap, then it will use UpdateWeightageForAccess
    assert(oldCached ne storeObject)
    if (oldCached ne null) {
      assert(evictionSet.remove(oldCached))
      release(oldCached)
    }
    assert(evictionSet.add(storeObject))
    true
  }

  @GuardedBy("evictionLock")
  private def updateWeightage(
      storeObject: StoredCacheObject[_ <: CacheValue],
      addedWeightage: Double,
      isBoost: Boolean): Boolean = {
    if (evictionSet.remove(storeObject)) {
      // remove the additional boost provided before the block was scanned and since this
      // is run by a single thread so there is no race condition in reading+writing
      val delta = addedWeightage + storeObject.weightageWithoutBoost - storeObject.weightage
      if (isBoost) storeObject.addGenerationalBoost(delta)
      else storeObject.weightage += delta
      assert(evictionSet.add(storeObject))
      true
    } else false
  }

  @GuardedBy("evictionLock")
  private def flushPendingActions(blocking: Boolean): Boolean = {
    // avoid locking the queue for entire duration since each work item in the queue can be
    // expensive, so block only when beyond a count or explicit flag has been passed
    if (blocking) {
      pendingActionLock.writeLock().lock()
      runIgnoreException(() => flushAllPendingActions())
    } else {
      var numItems = 0
      val iter = pendingActions.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getKey
        val value = entry.getValue
        runIgnoreException(value)
        pendingActions.remove(key, value)
        numItems += 1
        if (numItems >= 20) { // blocking run
          pendingActionLock.writeLock().lockInterruptibly()
          try {
            flushAllPendingActions()
          } finally {
            pendingActionLock.writeLock().unlock()
          }
        }
      }
      numItems > 0
    }
  }

  @GuardedBy("evictionLock")
  private def flushAllPendingActions(): Boolean = {
    if (pendingActions.isEmpty) false
    else {
      val iter = pendingActions.values().iterator()
      while (iter.hasNext) {
        runIgnoreException(iter.next())
      }
      pendingActions.clear()
      true
    }
  }

  @GuardedBy("evictionLock")
  private def flushPendingCompressedObjects(
      objects: java.util.ArrayList[CompressedCacheObject[C, D]]): Boolean = {
    val numObjects = objects.size()
    if (numObjects != 0) {
      var i = 0
      while (i < numObjects) {
        val p = objects.get(i)
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
     * Extra `weightage` to be added to [[storeObject]] as a result of execution of this action.
     */
    def objectWeightage: Double

    /**
     * The result of merging two [[PendingAction]]s for the same `key`.
     */
    def merge[U <: CacheValue](other: PendingAction[U]): PendingAction[_ <: CacheValue]

    /**
     * Abort the action.
     */
    def abort(): Unit
  }

  private final class CachePending[T <: CacheValue](
      override val storeObject: StoredCacheObject[T],
      private val timeWeightage: Double,
      private var existingStoreObject: StoredCacheObject[_ <: CacheValue])
      extends PendingAction[T] {

    override def objectWeightage: Double = 0.0

    @GuardedBy("evictionLock")
    override def apply(): Boolean = {
      val evicted = evictedEntries.remove(storeObject.key)
      if (evicted ne null) {
        // add the old statistical weightage which is always for decompressed object since
        // compressionSavings is already present in storeObject is required
        storeObject.weightage += evicted.weightage
        // evicted object being resurrected so the generation will increase
        storeObject.generation = evicted.generation + 1
      }
      storeObject.weightage += timeWeightage
      // only cache if the weightage is at least more than the entry with smallest weightage
      if (smallestWeightage() < storeObject.weightage) {
        putIntoCache(storeObject)
        true
      } else {
        release(storeObject)
        // touch the existing object if caching of new one failed
        if (existingStoreObject ne null) {
          updateWeightage(existingStoreObject, timeWeightage, isBoost = false)
        }
        false
      }
    }

    override def merge[U <: CacheValue](
        other: PendingAction[U]): PendingAction[_ <: CacheValue] = {
      other match {
        case null => this
        case c: CachePending[_] =>
          // storeObjects in CachePending are not in cacheMap so they cannot be the same
          assert(storeObject ne c.storeObject)
          storeObject.weightage += c.timeWeightage
          if (existingStoreObject eq null) existingStoreObject = c.existingStoreObject
          c.abort()
          this
        case u: UpdateWeightageForAccess[_] =>
          // storeObject cannot have a boost since it is not in the cache yet
          storeObject.weightage += u.objectWeightage
          this
      }
    }

    override def abort(): Unit = release(storeObject, markEvicted = false)
  }

  private final class UpdateWeightageForAccess[T <: CacheValue](
      override val storeObject: StoredCacheObject[T],
      private[this] var addedWeightage: Double)
      extends PendingAction[T] {

    override def objectWeightage: Double = addedWeightage

    @GuardedBy("evictionLock")
    override def apply(): Boolean = {
      updateWeightage(storeObject, addedWeightage, isBoost = false)
    }

    override def merge[U <: CacheValue](
        other: PendingAction[U]): PendingAction[_ <: CacheValue] = {
      other match {
        case null => this
        case c: CachePending[_] => c.merge(this)
        case u: UpdateWeightageForAccess[_] =>
          addedWeightage += u.objectWeightage
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
