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

import io.spark.lightspeed.memory._

import org.apache.spark.internal.Logging

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
    decompressionToDiskReadCost: Double,
    maxCompressionRatioToCache: Double,
    maxMemoryOverflowBeforeEvict: Double)
    extends EvictionManager[C, D]
    with Logging {

  def this(maxMemorySize: Long, millisForTwiceFreq: Long) = {
    this(
      maxMemorySize,
      millisForTwiceFreq,
      decompressionToDiskReadCost = 0.2,
      maxCompressionRatioToCache = 0.8,
      maxMemoryOverflowBeforeEvict = 0.05)
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
   * Lock acquired by [[runEviction]] for synchronization.
   * Used instead of a synchronized block to enable use of tryLock().
   */
  private[this] val evictionLock = new ReentrantLock()

  private[this] val currentTimeAdjustment: Double = {
    val currentTime = System.currentTimeMillis()
    val twiceFreqTime = currentTime - millisForTwiceFreq
    math.log(timeToDouble(twiceFreqTime) * 2.0) / math.log(timeToDouble(currentTime))
  }

  private[this] val cacheMap =
    new ConcurrentHashMap[Comparable[AnyRef], StoredCacheObject[_ <: CacheValue]]()
  // Note: need to add a "strong" reference to value somewhere else value can get GCed at any
  // point, hence the value in the evictionMap.
  private[this] val evictionMap =
    new ConcurrentSkipListMap[StoredCacheObject[_ <: CacheValue], CacheValue](
      (o1: StoredCacheObject[_ <: CacheValue], o2: StoredCacheObject[_ <: CacheValue]) => {
        if (o1 ne o2) {
          val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
          if (cmp != 0) cmp else o1.key.compareTo(o2.key.asInstanceOf[AnyRef])
        } else 0
      })

  /**
   * A blocking queue used to hold pending caching work. Callers should ensure that
   * [[usedMemory]] should not exceed [[maxMemory]] by more than
   * [[maxMemoryOverflowBeforeEvict]] before pushing new work items into this queue else thread
   * should either fail the caching work or block on [[runEviction]].
   */
  private[this] val pendingWorkQueue = new java.util.ArrayDeque[() => Unit](1024)

  /**
   * The [[runEviction]] method will set this flag to block access to [[pendingWorkQueue]]
   * after it has run some number of iterations through that queue.
   */
  private[this] var pendingQueueAvailable = true

  private def timeToDouble(millis: Long): Double = millis.toDouble / 1000000000.0

  private def millisTimeAdjusted(timeInMillis: Long): Double =
    math.pow(timeToDouble(timeInMillis), currentTimeAdjustment)

  private def addCompressionSavingsToWeightage(
      currentMillisAdjusted: Double,
      compressed: CompressedCacheObject[_ <: CacheValue, _ <: CacheValue],
      compressedSize: Long,
      decompressedSize: Long): Unit = {
    val compressionSavingsFactor = (decompressedSize.toDouble / compressedSize.toDouble) *
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
    require(either.isLeft || !value.isCompressed)
    require(either.isRight || value.isCompressed)
    require(memorySize > 0L)
    require(transformer ne null)
    require(transformer.compressionAlgorithm ne null)
    // invariant to check for case when there are no compressed/decompressed versions of the object
    require(transformer.compressionAlgorithm.isDefined || !value.isCompressed)

    maxMemoryLock.readLock().lock()
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
        value.startWork() // value will be put in cache so mark as in-use
        // put into the cacheMap in any case, then update the evictionMap in runEviction either
        // synchronously, if the evictionLock can be acquired immediately or else asynchronously
        if (cacheMap.putIfAbsent(cached.key, cached) eq null) {
          usedMemory.addAndGet(memorySize)
          cacheAndEvict(() => evictionMap.put(cached, value), timestamp)
          true
        } else false
      } else false
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  override def getDecompressed(
      key: Comparable[AnyRef],
      timestamp: Long,
      loader: Option[Comparable[AnyRef] => Option[(Either[C, D], TransformValue[C, D])]])
    : Option[D] = {

    val cached = cacheMap.get(key)
    val cachedValue = if (cached ne null) cached.value else null
    if (cachedValue ne null) cached match {
      case d: DecompressedCacheObject[D, C] =>
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
        Some(cachedValue.asInstanceOf[D])

      case c: CompressedCacheObject[C, D] =>
        val (d, dv) = c.decompress(cachedValue.asInstanceOf[C])
        val decompressedSize = dv.memorySize
        // determine if decompressed object should be cached immediately
        d.weightage = c.weightage - c.compressionSavings
        d.weightage += millisTimeAdjusted(timestamp)
        // quick check to determine if the object being inserted has a lower weightage than
        // the smallest one in the evictionMap; in the worst case this can still end up
        // putting decompressed object in cache while runEviction can remove it or
        // change to compressed form again
        if (d.weightage >= c.weightage || evictionMap.firstKey().weightage < d.weightage) {
          maxMemoryLock.readLock().lock()
          try {
            if (decompressedSize < maxMemory) {
              dv.startWork() // value will be put in cache so mark as in-use
              val cc = cacheMap.put(key, d).asInstanceOf[StoredCacheObject[CacheValue]]
              val oldValue = if (cc ne null) cc.value else null
              if (oldValue ne null) {
                cc.clearValue()
                usedMemory.addAndGet(decompressedSize - oldValue.memorySize)
                oldValue.endWork()
                oldValue.release(c.transformer, 0L)
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
        Some(dv)
    } else {
      loader match {
        case Some(l) =>
          l(key) match {
            case Some((either, transformer)) =>
              cacheObject(key, either, transformer, timestamp, throwOnLargeObject = false)
              // directly return the loaded object decompressing if required
              either match {
                case Left(value) => Some(transformer.decompress(value))
                case Right(value) => Some(value)
              }
            case _ => None
          }

        case _ => None
      }
    }
  }

  /**
   * Convenience method to either perform caching related work followed by eviction, or else
   * push the work item into the [[pendingWorkQueue]] queue.
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] has been acquired
   * before invoking this method.
   */
  private def cacheAndEvict(doCache: () => Unit, timestamp: Long): Unit = {
    if (runEviction(blocking = false, doCache, timestamp) == Long.MinValue) {
      // check if usedMemory is over maxMemory by too much
      val used = usedMemory.get()
      if (used.toDouble >= (1.0 + maxMemoryOverflowBeforeEvict) * maxMemory.toDouble ||
          !enqueueWork(doCache)) {
        runEviction(blocking = true, doCache, timestamp)
      }
    }
  }

  private def enqueueWork(work: () => Unit): Boolean = pendingWorkQueue.synchronized {
    pendingQueueAvailable && pendingWorkQueue.size() < 1024 && pendingWorkQueue.offer(work)
  }

  /**
   * Run the eviction loop in order to bring [[usedMemory]] below [[maxMemory]] and clear up any
   * work items in [[pendingWorkQueue]]. Only one thread can perform eviction at a time and
   * in order to minimize the bottleneck of multiple threads waiting for [[runEviction]], the
   * other threads should queue up their work in [[pendingWorkQueue]] as long as [[usedMemory]]
   * has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]] (for the latter
   * case the thread will need to either wait for [[runEviction]] to complete or skip caching).
   *
   * NOTE: Callers MUST ensure that [[maxMemoryLock.readLock()]] or [[maxMemoryLock.writeLock()]]
   * has been acquired before invoking this method.
   *
   * @param blocking when true then [[evictionLock]] is acquired in a blocking manner else a
   *                 `tryLock` with zero timeout is attempted
   * @param currentWork the work required to be done by current thread if [[evictionLock]]1111
   *                    was acquired, else caller is supposed to queue it in [[pendingWorkQueue]]
   *                    and let the other thread that successfully acquires it execute the method
   * @param timestamp the current timestamp which should be the [[System.currentTimeMillis()]]
   *                  at the start of operation
   * @param maxFlushPending skip and block flushPendingWork after these many iterations
   *
   * @return if [[evictionLock.tryLock]] was acquired then the number of bytes evicted else
   *         [[Long.MinValue]] if eviction was skipped (in which case the caller is supposed
   *         to queue the `currentWork` in [[pendingWorkQueue]] if the total used memory
   *         has not exceeded [[maxMemory]] by more than [[maxMemoryOverflowBeforeEvict]]
   */
  private def runEviction(
      blocking: Boolean,
      currentWork: () => Unit,
      timestamp: Long,
      maxFlushPending: Int = 20): Long = {
    if (blocking) evictionLock.lockInterruptibly()
    else if (!evictionLock.tryLock()) {
      return Long.MinValue // indicates that runEviction was skipped
    }
    val pendingCompressedObjects = new java.util.ArrayList[(CompressedCacheObject[C, D], C)]()
    var remainingFlushes = maxFlushPending
    try {
      val max = maxMemory
      var evicted = 0L

      if (remainingFlushes >= 0) flushPendingWork(remainingFlushes == 0)
      if (currentWork ne null) currentWork()
      if (max >= usedMemory.get()) return evicted

      var iter = evictionMap.keySet().iterator()
      while (iter.hasNext) {
        val candidate = iter.next()
        val removeKey = candidate.key
        if (remainingFlushes >= 0 && flushPendingWork(remainingFlushes == 0)) {
          // evictionMap changed so restart the iterator
          iter = evictionMap.keySet().iterator()
          remainingFlushes -= 1
        } else {
          val removed = cacheMap.remove(removeKey).asInstanceOf[StoredCacheObject[CacheValue]]
          // remove from evictionMap after cacheMap since it contains hard-reference to object
          // and so there is a minuscule chance that it gets GCed before removed.get() call
          val removedVal = if (removed ne null) removed.value else null
          iter.remove()
          if (removedVal ne null) {
            var removedSize = removedVal.memorySize
            val transformer =
              removed.transformer.asInstanceOf[TransformValue[CacheValue, CacheValue]]
            removed.clearValue()
            // for decompressed blocks, compress and put them back rather than evicting entirely
            // also check if the evictionMap contains compressed version which means that
            // getDecompressed already queued a task to push in decompressed object into the
            // evictionMap with higher weightage so skip this object and deal in next round
            if (!removed.isCompressed && !candidate.isCompressed &&
                transformer.compressionAlgorithm.isDefined &&
                (transformer.compressedSize(removedVal).toDouble / removedSize.toDouble) <=
                  maxCompressionRatioToCache) {
              val p @ (compressed, compressedVal) = removed
                .asInstanceOf[DecompressedCacheObject[D, C]]
                .compress(removedVal.asInstanceOf[D])
              val compressedSize = compressedVal.memorySize
              if (removedSize > compressedSize) {
                compressed.weightage = removed.weightage
                addCompressionSavingsToWeightage(
                  millisTimeAdjusted(timestamp),
                  compressed,
                  compressedSize,
                  removedSize)
                compressedVal.startWork() // value will be put in cache so mark as in-use
                if (cacheMap.putIfAbsent(compressed.key, compressed) eq null) {
                  pendingCompressedObjects.add(p)
                  removedSize -= compressedSize
                }
              }
            }
            removedVal.endWork()
            removedVal.release(transformer, 0L)
            evicted += removedSize
            usedMemory.addAndGet(-removedSize)
          }
          // break the loop if usedMemory has fallen below maxMemory
          if (max >= usedMemory.get()) return evicted
        }
      }
      // insert the pending compressed objects and try again
      if (flushPendingCompressedObjects(pendingCompressedObjects)) {
        evicted + runEviction(blocking = true, currentWork = null, timestamp, remainingFlushes)
      } else evicted
    } finally {
      flushPendingCompressedObjects(pendingCompressedObjects)
      evictionLock.unlock()
      if (remainingFlushes <= 0) pendingWorkQueue.synchronized {
        pendingQueueAvailable = true
      }
    }
  }

  private def flushPendingWork(blockQueue: Boolean): Boolean = {
    // avoid locking the queue for entire duration since each work item in the queue can be
    // expensive, so block only during poll but once beyond a count then do blocking iteration
    var workItem: () => Unit = null
    pendingWorkQueue.synchronized {
      if (blockQueue) pendingQueueAvailable = false
      if (pendingWorkQueue.isEmpty) return false
      workItem = pendingWorkQueue.poll()
    }
    if (workItem ne null) {
      var numItems = 1
      do {
        workItem()
        workItem = pendingWorkQueue.synchronized(pendingWorkQueue.poll())
        numItems += 1
      } while ((workItem ne null) && numItems < 20)
      // full blocking if queue is still not empty
      if (workItem ne null) pendingWorkQueue.synchronized {
        do {
          workItem()
          workItem = pendingWorkQueue.poll()
        } while (workItem ne null)
        pendingWorkQueue.clear()
      }
      true
    } else false
  }

  private def flushPendingCompressedObjects(
      objects: java.util.ArrayList[(CompressedCacheObject[C, D], C)]): Boolean = {
    val numObjects = objects.size()
    if (numObjects != 0) {
      var i = 0
      while (i < numObjects) {
        val p = objects.get(i)
        i += 1
        try {
          evictionMap.put(p._1, p._2)
        } catch {
          case t: Throwable =>
            // remove from cacheMap too, log and move to next
            cacheMap.remove(p._1)
            logError("Unexpected exception in evictionMap.put", t)
        }
      }
      objects.clear()
      true
    } else false
  }
}
