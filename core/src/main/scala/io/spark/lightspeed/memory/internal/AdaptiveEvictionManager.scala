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

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import io.spark.lightspeed.memory._

/**
 * An Adaptive [[EvictionManager]] that uses a combination of LRU and MFU where aging leads to
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
 * or higher priority compressed block
 *
 * If evicted rather than compressed then:
 * - cost to read compressed block from disk on every read
 *
 * In the end when memory runs out, then maintaining a mix of high priority decompressed blocks
 * plus remaining compressed blocks seems to be the best bet. How to determine this balance i.e.
 * cut-off point for the 'priority' or 'weightage' as the implementation is calculating.
 * The above factors should determine that but in all cases it can be assumed that caching a
 * compressed block in memory is definitely cheaper than reading from disk everytime.
 *
 * For other cases extra factors in `weightage` for:
 *
 * a) cost of decompressing a compressed block will lower `weightage`
 * b) higher degree of compression will increase `weightage`. Of course, recent usage will
 * lead to increase in `weightage` as it does now but will also take into account these two
 * factors for compressed blocks.
 *
 * For a), the cost of decompressing (i.e. millis required in wall clock time for a given
 * decompressed output size) will be noted for a compression algorithm the first time and
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
 * @param maxCompressionRatioToCache maximum ratio of compressed to decompressed size of a block to
 *                                   cache the compressed form transparently rather than evicting
 * @param maxMemoryOverflowBeforeEvict maximum ratio of `maxMemorySize` that can overflow before
 *                                     eviction would forcefully block all putter/getter threads
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
final class AdaptiveEvictionManager[C <: CacheValue, D <: CacheValue](
    maxMemorySize: Long,
    millisForTwiceFreq: Long,
    decompressionToDiskReadCost: Double = 0.2,
    maxCompressionRatioToCache: Double = 0.8,
    maxMemoryOverflowBeforeEvict: Double = 0.05)
    extends EvictionManager[C, D] {

  require(maxMemorySize > 0L, s"maximum memory size is $maxMemorySize")
  require(
    millisForTwiceFreq > 0L,
    s"millis parameter to adjust weightage of timestamp is $millisForTwiceFreq")
  require(
    decompressionToDiskReadCost > 0.0 && decompressionToDiskReadCost < 1.0,
    "ratio of cost to decompress a disk block to reading from disk should be (0.0, 1.0) " +
      s"but was $decompressionToDiskReadCost")
  require(
    maxCompressionRatioToCache > 0.0 && maxCompressionRatioToCache < 1.0,
    "maximum ratio of compressed to decompressed size for caching should be (0.0, 1.0) " +
      s"but was $maxCompressionRatioToCache")
  require(
    maxMemoryOverflowBeforeEvict > 0.0 && maxMemoryOverflowBeforeEvict < 1.0,
    "maximum ratio that can overflow before blocking for eviction should be (0.0, 1.0) " +
      s"but was $maxMemoryOverflowBeforeEvict")

  /** should always be accessed within [[maxMemoryLock]] */
  private[this] var maxMemory: Long = maxMemorySize
  private[this] val maxMemoryLock = new ReentrantReadWriteLock()
  private[this] val usedMemory = new AtomicLong(0L)

  /**
   * A blocking queue used to hold pending caching work. Callers should ensure that
   * [[usedMemory]] should not exceed [[maxMemory]] by more than
   * [[maxMemoryOverflowBeforeEvict]] before pushing new work items into this queue else thread
   * should either fail the caching work or block on [[runEviction]].
   */
  private[this] val pendingCachingWork = new ArrayBlockingQueue[() => Unit](1024)

  /**
   * Lock acquired by [[runEviction]] to ensure only a single thread runs it.
   */
  private[this] val evictionLock = new ReentrantLock()

  private[this] val currentTimeAdjustment: Double = {
    val currentTime = System.currentTimeMillis()
    val twiceFreqTime = currentTime - millisForTwiceFreq
    math.log(timeToDouble(twiceFreqTime) * 2.0) / math.log(timeToDouble(currentTime))
  }

  private[this] val cacheMap = new ConcurrentHashMap[Comparable[AnyRef], StoredCacheObject[_]]()
  // Note: need to add a "strong" reference to value somewhere else value can get GC'd at any
  // point, hence the value in the evictionMap.
  private[this] val evictionMap = new ConcurrentSkipListMap[StoredCacheObject[_], CacheValue](
    (o1: StoredCacheObject[_], o2: StoredCacheObject[_]) => {
      if (o1 ne o2) {
        val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
        if (cmp != 0) cmp else o1.key.compareTo(o2.key)
      } else 0
    })

  private def timeToDouble(millis: Long): Double = millis.toDouble / 1000000000.0

  private def millisTimeAdjusted(timeInMillis: Long): Double =
    math.pow(timeToDouble(timeInMillis), currentTimeAdjustment)

  private def addCompressionSavingsToWeightage(
      currentMillisAdjusted: Double,
      compressed: CompressedCacheObject[_, _],
      decompressedSize: Long): Unit = {
    val compressionSavingsFactor = (decompressedSize.toDouble / compressed.memorySize.toDouble) *
      (1.0 - decompressionToDiskReadCost)
    if (compressionSavingsFactor > 1.0) {
      compressed.compressionSavings = currentMillisAdjusted * compressionSavingsFactor
      compressed.weightage += compressed.compressionSavings
    }
  }

  override def setLimit(newMaxMemory: Long, timestamp: Long): Unit = {
    maxMemoryLock.writeLock().lock()
    try {
      maxMemory = newMaxMemory
      runEviction(blocking = true, currentWork = null, timestamp)
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putObject[T <: CacheValue, U <: CacheValue](
      obj: PublicCacheObject[T, U],
      timestamp: Long): Boolean = {

    require(obj.key ne null)
    require(obj.value ne null)
    require(obj.memorySize > 0L)
    require(obj.compressionAlgorithm ne null)
    require(obj.otherObjectSize > 0)
    require(obj.toOtherObject ne null)
    require(obj.fromOtherObject ne null)
    require(obj.stored.isEmpty)

    maxMemoryLock.readLock().lock()
    try {
      val objectSize = obj.memorySize
      if (objectSize >= maxMemory) {
        throw new IllegalArgumentException(
          s"Cannot put object of $objectSize bytes with maxMemory = $maxMemory")
      }
      val cached = obj.toStoredObject
      cached.weightage = millisTimeAdjusted(timestamp)
      // if this is a compressed object, then add the savings due to compression to weightage
      if (obj.isCompressed) {
        addCompressionSavingsToWeightage(
          cached.weightage,
          cached.asInstanceOf[CompressedCacheObject[T, U]],
          obj.otherObjectSize)
      }
      // quick check to determine if the object being inserted has a lower weightage than
      // the smallest one in the evictionMap
      if (evictionMap.firstKey().weightage < cached.weightage) {
        cached.startWork() // value will be put in cache so mark as in-use
        // put into the cacheMap in any case, then update the evictionMap in runEviction either
        // synchronously, if the evictionLock can be acquired immediately or else asynchronously
        if (cacheMap.putIfAbsent(cached.key, cached) eq null) {
          usedMemory.addAndGet(objectSize)
          cacheAndEvict(() => evictionMap.put(cached, obj.value), timestamp)
          true
        } else false
      } else false
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  override def getDecompressed(
      key: Comparable[AnyRef],
      timestamp: Long): Option[PublicCacheObject[D, C]] = cacheMap.get(key) match {

    case null => None

    case d: DecompressedCacheObject[D, C] =>
      val dv = d.get()
      if (dv ne null) {
        // update weightage for access and need to remove+put into evictionMap to reorder
        maxMemoryLock.readLock().lock()
        try {
          cacheAndEvict(() => {
            val value = evictionMap.remove(d)
            if (value ne null) {
              d.weightage += millisTimeAdjusted(timestamp)
              evictionMap.put(d, value)
            }
          }, timestamp)
        } finally {
          maxMemoryLock.readLock().unlock()
        }
        Some(
          new PublicCacheObject[D, C](
            d.key,
            dv,
            d.memorySize,
            d.isCompressed,
            d.compressionAlgorithm,
            d.compressedSize,
            d.compressObject,
            d.decompressObject,
            d.doFinalize,
            Some(d)))
      } else None

    case c: CompressedCacheObject[C, D] =>
      val cv = c.get()
      if (cv ne null) {
        val (d, dv) = c.decompress(cv)
        val decompressedSize = d.memorySize
        // determine if decompressed object should be cached immediately
        d.weightage = c.weightage - c.compressionSavings + millisTimeAdjusted(timestamp)
        // quick check to determine if the object being inserted has a lower weightage than
        // the smallest one in the evictionMap
        if (d.weightage >= c.weightage || evictionMap.firstKey().weightage < d.weightage) {
          maxMemoryLock.readLock().lock()
          try {
            if (decompressedSize < maxMemory) {
              d.startWork() // value will be put in cache so mark as in-use
              val cc = cacheMap.put(key, d)
              if (cc ne null) {
                usedMemory.addAndGet(decompressedSize - cc.memorySize)
                cc.endWork()
                cc.release(0L)
              } else usedMemory.addAndGet(decompressedSize)
              cacheAndEvict(() => {
                evictionMap.remove(cc)
                evictionMap.put(d, dv)
              }, timestamp)
            }
          } finally {
            maxMemoryLock.readLock().unlock()
          }
        }
        Some(
          new PublicCacheObject[D, C](
            d.key,
            dv,
            decompressedSize,
            d.isCompressed,
            d.compressionAlgorithm,
            d.compressedSize,
            d.compressObject,
            d.decompressObject,
            d.doFinalize,
            Some(d)))
      } else None
  }

  /**
   * Convenience method to either perform caching related work followed by eviction, or else
   * push the work item into the [[pendingCachingWork]] queue.
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] has been acquired
   * before invoking this method.
   */
  private def cacheAndEvict(doCache: () => Unit, timestamp: Long): Unit = {
    if (runEviction(blocking = false, doCache, timestamp) == Long.MinValue) {
      // check if usedMemory is over maxMemory by too much
      val used = usedMemory.get()
      if (used.toDouble >= (1.0 + maxMemoryOverflowBeforeEvict) * maxMemory.toDouble ||
          !pendingCachingWork.offer(doCache)) {
        runEviction(blocking = true, doCache, timestamp)
      }
    }
  }

  /**
   * Run the eviction loop in order to bring [[usedMemory]] below [[maxMemory]] and clear up any
   * work items in [[pendingCachingWork]]. Only one thread can perform eviction at a time and
   * in order to minimize the bottleneck of multiple threads waiting for [[runEviction]], the
   * other threads should queue up their work in [[pendingCachingWork]] as long as [[usedMemory]]
   * has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] (for the latter
   * case the thread will need to either wait for [[runEviction]] to complete or skip caching).
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] or [[maxMemoryLock.writeLock()]]
   * has been acquired before invoking this method.
   *
   * @param blocking when true then [[evictionLock]] is acquired in a blocking manner else a
   *                 `tryLock` with zero timeout is attempted
   * @param currentWork the work required to be done by current thread if [[evictionLock]]1111
   *                    was acquired, else caller is supposed to queue it in [[pendingCachingWork]]
   *                    and let the other thread that successfully acquires it execute the method
   * @param timestamp the current timestamp which should be the [[System.currentTimeMillis()]]
   *                  at the start of operation
   *
   * @return if [[evictionLock.tryLock]] was acquired then the number of bytes evicted else
   *         [[Long.MinValue]] if eviction was skipped (in which case the caller is supposed
   *         to queue the `currentWork` in [[pendingCachingWork]] if the total used memory
   *         has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]]
   */
  private def runEviction(blocking: Boolean, currentWork: () => Unit, timestamp: Long): Long = {
    if (blocking) evictionLock.lockInterruptibly()
    else if (!evictionLock.tryLock()) {
      return Long.MinValue // indicates that runEviction was skipped
    }
    try {
      val max = maxMemory
      var evicted = 0L
      var maxRuns = 20 // single thread should not be stuck in eviction for too long

      flushPendingWork()
      if (currentWork ne null) currentWork()
      if (max >= usedMemory.get()) return evicted

      var iter = evictionMap.keySet().iterator()
      while (maxRuns > 0 && !cacheMap.isEmpty && iter.hasNext) {
        val removeKey = iter.next().key
        if (flushPendingWork()) {
          // evictionMap changed so restart the iterator
          iter = evictionMap.keySet().iterator()
          maxRuns -= 1
        } else {
          iter.remove()
          val removed = cacheMap.remove(removeKey)
          if (removed ne null) {
            val removedVal = removed.get()
            var removedSize = removed.memorySize
            // for decompressed blocks, compress and put them back rather than evicting entirely
            if (!removed.isCompressed && removedVal != null &&
                (removed.compressedSize.toDouble / removed.memorySize.toDouble) <=
                  maxCompressionRatioToCache) {
              val (compressed, compressedVal) = removed
                .asInstanceOf[DecompressedCacheObject[D, C]]
                .compress(removedVal.asInstanceOf[D])
              if (removedSize > compressed.memorySize) {
                compressed.weightage = removed.weightage
                addCompressionSavingsToWeightage(
                  millisTimeAdjusted(timestamp),
                  compressed,
                  removedSize)
                if (cacheMap.putIfAbsent(compressed.key, compressed) eq null) {
                  evictionMap.put(compressed, compressedVal)
                  removedSize -= compressed.memorySize
                }
              }
            }
            removed.endWork()
            removed.release(0L)
            evicted += removedSize
            usedMemory.addAndGet(-removedSize)
          }
          if (max >= usedMemory.get()) return evicted
        }
      }
      evicted
    } finally {
      evictionLock.unlock()
    }
  }

  private def flushPendingWork(): Boolean = {
    val size = pendingCachingWork.size()
    if (size != 0) {
      val workItems = new java.util.ArrayList[() => Unit](size + 1)
      pendingCachingWork.drainTo(workItems)
      val numItems = workItems.size()
      if (numItems != 0) {
        var i = 0
        while (i < numItems) {
          workItems.get(i)()
          i += 1
        }
        true
      } else false
    } else false
  }
}
