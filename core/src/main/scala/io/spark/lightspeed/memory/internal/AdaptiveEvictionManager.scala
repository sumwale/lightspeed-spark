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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import io.spark.lightspeed.memory.{CacheObject, CompressedCacheObject, DecompressedCacheObject, EvictionManager}

class AdaptiveEvictionManager[C, D](_maxMemorySize: Long, _millisForTwiceFreq: Long)
    extends EvictionManager[C, D] {

  private[this] final var maxMemory: Long = _maxMemorySize
  private[this] final val maxMemoryLock = new ReentrantReadWriteLock()
  private[this] final val usedMemory = new AtomicLong(0L)

  private[this] final val currentTimeAdjustment: Double = {
    val currentTime = System.currentTimeMillis()
    val twiceFreqTime = currentTime - _millisForTwiceFreq
    math.log(timeToDouble(twiceFreqTime) * 2.0) / math.log(timeToDouble(currentTime))
  }

  private[this] final val cacheMap = new ConcurrentHashMap[AnyRef, CacheObject[_]]()
  private[this] final val evictionMap =
    new ConcurrentSkipListSet[CacheObject[_]]((o1: CacheObject[_], o2: CacheObject[_]) => {
      if (o1 ne o2) {
        val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
        if (cmp != 0) cmp else o1.key.compareTo(o2.key)
      } else 0
    })

  private final def timeToDouble(millis: Long): Double = millis.toDouble / 1000000000.0

  private final def currentTimeAdjusted(): Double =
    math.pow(timeToDouble(System.currentTimeMillis()), currentTimeAdjustment)

  override def setLimit(maxMemorySize: Long): Unit = {
    maxMemoryLock.writeLock().lock()
    try {
      maxMemory = maxMemorySize
      doEvict()
    } finally {
      maxMemoryLock.writeLock().unlock()
    }
  }

  override def putCompressed(obj: CompressedCacheObject[C, D]): Boolean = {
    maxMemoryLock.readLock().lock()
    try {
      val max = maxMemory
      val size = obj.memorySize
      if (size > max) {
        throw new IllegalArgumentException(
          s"Cannot put object of $size bytes with maxMemory = $max")
      }
      obj.managed = true
      obj.weightage = currentTimeAdjusted()
      if (cacheMap.putIfAbsent(obj.key, obj) eq null) {
        evictionMap.add(obj)
        val used = usedMemory.addAndGet(size)
        if (used > max) doEvict()
        true
      } else {
        obj.reset()
        false
      }
    } finally {
      maxMemoryLock.readLock().unlock()
    }
  }

  private def adjustWeightageForAccess(c: CacheObject[_]): Unit = {
    evictionMap.remove(c)
    c.weightage += currentTimeAdjusted()
    evictionMap.add(c)
  }

  override def getCompressed(key: Comparable[AnyRef]): Option[CompressedCacheObject[C, D]] = {
    cacheMap.get(key) match {
      case null => None
      case c: CompressedCacheObject[C, D] => adjustWeightageForAccess(c); Option(c)
      case d: DecompressedCacheObject[D, C] => adjustWeightageForAccess(d); Option(d.compress())
    }
  }

  override def getDecompressed(key: Comparable[AnyRef]): Option[DecompressedCacheObject[D, C]] = {
    cacheMap.get(key) match {
      case null => None
      case d: DecompressedCacheObject[D, C] => adjustWeightageForAccess(d); Option(d)
      case c: CompressedCacheObject[C, D] =>
        // replace compressed object with decompressed one
        val d = c.decompress()
        d.managed = true
        d.weightage = c.weightage + currentTimeAdjusted()
        if (cacheMap.remove(key) ne null) {
          evictionMap.remove(key)
          c.managed = false
          c.release(0L)
          usedMemory.addAndGet(-c.memorySize)
        }
        if (cacheMap.putIfAbsent(key, d) eq null) {
          evictionMap.add(d)
          usedMemory.addAndGet(d.memorySize)
          doEvict()
        } else {
          d.reset()
        }
        Option(d)
    }
  }

  private def doEvict(): Long = synchronized {
    val max = maxMemory
    if (max >= usedMemory.get()) return 0
    var evicted = 0L
    while (!cacheMap.isEmpty) {
      val iter = evictionMap.iterator()
      while (iter.hasNext) {
        val toRemove = iter.next()
        val removedSize = toRemove.memorySize
        iter.remove()
        cacheMap.remove(toRemove.key)
        toRemove.managed = false
        toRemove.release(0L)
        evicted += removedSize
        if (max >= usedMemory.addAndGet(-removedSize)) return evicted
      }
    }
    evicted
  }
}
