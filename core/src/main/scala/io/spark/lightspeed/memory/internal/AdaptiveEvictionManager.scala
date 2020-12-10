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
import java.util.concurrent.locks.ReentrantReadWriteLock

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
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
final class AdaptiveEvictionManager[C, D](
    maxMemorySize: Long,
    millisForTwiceFreq: Long,
    decompressionToDiskReadCost: Double = 0.2,
    maxCompressionRatioToCache: Double = 0.8)
    extends EvictionManager[C, D] {

  require(maxMemorySize > 0L, s"maximum memory size is $maxMemorySize")
  require(
    millisForTwiceFreq > 0L,
    s"millis parameter to adjust weightage of timestamp is $millisForTwiceFreq")
  require(
    decompressionToDiskReadCost > 0.0 && decompressionToDiskReadCost < 1.0,
    "ratio of cost to decompress a disk block to reading from disk should be (0.0, 1.0) " +
      s"but was $decompressionToDiskReadCost")

  private[this] val evictionLock = new ReentrantReadWriteLock()
  private[this] val maxMemoryLock = new ReentrantReadWriteLock()

  /** should always be accessed within [[maxMemoryLock]] */
  private[this] var maxMemory: Long = maxMemorySize
  private[this] val usedMemory = new AtomicLong(0L)

  private[this] val currentTimeAdjustment: Double = {
    val currentTime = System.currentTimeMillis()
    val twiceFreqTime = currentTime - millisForTwiceFreq
    math.log(timeToDouble(twiceFreqTime) * 2.0) / math.log(timeToDouble(currentTime))
  }

  private[this] val cacheMap = new ConcurrentHashMap[AnyRef, StoredCacheObject[_, _]]()
  // Note: need to add a "strong" reference to value somewhere else value can get GC'd at any
  // point, hence the value in the evictionMap.
  private[this] val evictionMap =
    new ConcurrentSkipListMap[StoredCacheObject[_, _], Any](
      (o1: StoredCacheObject[_, _], o2: StoredCacheObject[_, _]) => {
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
      stored: StoredCacheObject[_, _],
      decompressedSize: Long): Unit = {
    val compressionSavingsFactor = (decompressedSize.toDouble / stored.memorySize.toDouble) *
      (1.0 - decompressionToDiskReadCost)
    if (compressionSavingsFactor > 1.0) {
      stored.weightage += currentMillisAdjusted * compressionSavingsFactor
    }
  }

  override def setLimit(newMaxMemory: Long): Unit = {
    maxMemoryLock.writeLock().lock()
    try {
      maxMemory = newMaxMemory
      doEvict()
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putObject[T, U](obj: PublicCacheObject[T, U]): Boolean = {
    require(obj.key ne null)
    require(obj.value != null)
    require(obj.memorySize > 0L)
    require(obj.compressionAlgorithm ne null)
    require(obj.otherObjectSize > 0)
    require(obj.toOtherObject ne null)
    require(obj.fromOtherObject ne null)
    require(obj.stored.isEmpty)

    var evictionReadLockAcquired = false
    maxMemoryLock.readLock().lock()
    try {
      val max = maxMemory
      val size = obj.memorySize
      if (size > max) {
        throw new IllegalArgumentException(
          s"Cannot put object of $size bytes with maxMemory = $max")
      }
      val cached = obj.toStoredObject
      cached.startWork()
      cached.weightage = millisTimeAdjusted(System.currentTimeMillis())
      // if this is a compressed object, then add the savings due to compression to weightage
      if (obj.isCompressed) {
        addCompressionSavingsToWeightage(cached.weightage, cached, obj.otherObjectSize)
      }
      // should acquire the eviction read lock before changing evictionMap
      evictionLock.readLock().lock()
      evictionReadLockAcquired = true
      if (cacheMap.putIfAbsent(cached.key, cached) eq null) {
        evictionMap.put(cached, obj.value)
        if (max < usedMemory.addAndGet(size)) {
          // release the eviction read lock before doEvict() that takes the write lock
          evictionLock.readLock().unlock()
          evictionReadLockAcquired = false
          doEvict()
        }
        true
      } else false
    } finally {
      if (evictionReadLockAcquired) evictionLock.readLock().unlock()
      maxMemoryLock.readLock().unlock()
    }
  }

  private def adjustWeightageForAccess(o: StoredCacheObject[_, _], timeInMillis: Long): Any = {
    // should acquire the eviction read lock before changing evictionMap
    evictionLock.readLock().lock()
    try {
      val value = evictionMap.remove(o)
      if (value != null) {
        o.weightage += millisTimeAdjusted(timeInMillis)
        evictionMap.put(o, value)
      }
      value
    } finally {
      evictionLock.readLock().unlock()
    }
  }

  override def getDecompressed(key: Comparable[AnyRef]): Option[PublicCacheObject[D, C]] = {
    cacheMap.get(key) match {
      case null => None
      case d: DecompressedCacheObject[D, C] =>
        val value = adjustWeightageForAccess(d, System.currentTimeMillis())
        if (value != null) {
          Some(
            new PublicCacheObject[D, C](
              d.key,
              value.asInstanceOf[D],
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
        if (cv != null) {
          var evictionReadLockAcquired = false
          maxMemoryLock.readLock().lock()
          try {
            val startTime = System.currentTimeMillis()
            val (d, dv) = c.decompress(cv)
            // determine if decompressed object should be cached immediately
            d.startWork()
            d.weightage = c.weightage + millisTimeAdjusted(startTime)
            // should acquire the eviction read lock before changing evictionMap
            evictionLock.readLock().lock()
            evictionReadLockAcquired = true
            if (cacheMap.remove(key) ne null) {
              evictionMap.remove(key)
              c.endWork()
              c.release(0L)
              usedMemory.addAndGet(-c.memorySize)
            }
            if (cacheMap.putIfAbsent(key, d) eq null) {
              evictionMap.put(d, dv)
              if (maxMemory < usedMemory.addAndGet(d.memorySize)) {
                // release the eviction read lock before doEvict() that takes the write lock
                evictionLock.readLock().unlock()
                evictionReadLockAcquired = false
                doEvict()
              }
            } else d.reset()
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
          } finally {
            if (evictionReadLockAcquired) evictionLock.readLock().unlock()
            maxMemoryLock.readLock().unlock()
          }
        } else None
    }
  }

  /**
   * Callers should ensure that `maxMemoryLock` is already acquired (either read or write lock)
   */
  private def doEvict(): Long = {
    evictionLock.writeLock().lock()
    try {
      val max = maxMemory
      if (max >= usedMemory.get()) return 0
      var evicted = 0L
      while (!cacheMap.isEmpty) {
        val iter = evictionMap.keySet().iterator()
        while (iter.hasNext) {
          val toRemove = iter.next()
          val toRemoveVal = toRemove.get()
          var removedSize = toRemove.memorySize
          iter.remove()
          cacheMap.remove(toRemove.key)
          // for decompressed blocks, compress and put them back rather than evicting entirely
          if (!toRemove.isCompressed && toRemoveVal != null && (toRemove.compressedSize.toDouble /
                toRemove.memorySize.toDouble) <= maxCompressionRatioToCache) {
            val (compressed, compressedVal) = toRemove
              .asInstanceOf[DecompressedCacheObject[D, C]]
              .compress(toRemoveVal.asInstanceOf[D])
            if (removedSize > compressed.memorySize) {
              compressed.weightage = toRemove.weightage
              addCompressionSavingsToWeightage(
                millisTimeAdjusted(System.currentTimeMillis()),
                compressed,
                removedSize)
              if (cacheMap.putIfAbsent(compressed.key, compressed) eq null) {
                evictionMap.put(compressed, compressedVal)
                removedSize -= compressed.memorySize
              }
            }
          }
          toRemove.endWork()
          toRemove.release(0L)
          evicted += removedSize
          if (max >= usedMemory.addAndGet(-removedSize)) return evicted
        }
      }
      evicted
    } finally {
      evictionLock.writeLock().unlock()
    }
  }
}
