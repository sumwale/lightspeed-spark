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

package com.github.spark.lightspeed.collection

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging

/**
 * A simple segmented implementation of a concurrent hash map. The overall hash map is divided
 * into a fixed number of segments (a power of 2) depending on desired concurrency level. Each
 * segment is simply a normal HashMap implementation which is protected by a read-write lock.
 *
 * Iterators have a slight complication with respect to the duration of locks which can otherwise
 * be indefinite, so each segment is copied to an array in the segment read lock by the iterator.
 * To avoid this copy overhead, the [[foreach]] method is overridden to hold the segment read locks
 * for the entire duration of the operation on a segment so this is supposed to be used for short
 * duration operations only else update operations can be blocked for a long time.
 *
 * @param initialCapacity hint for minimum number of elements in the map
 * @param loadFactor the load factor of the hash map
 * @param concurrencyLevel expected number of concurrent threads that can update the map
 * @param mapImpl a closure taking the `initialCapacity` and `loadFactor` arguments and returning
 *                the actual [[java.util.Map]] implementation to use
 *
 * @tparam K type of keys in the map
 * @tparam V type of values in the map
 */
final class ConcurrentSegmentedMap[K: ClassTag, V: ClassTag](
    initialCapacity: Int,
    loadFactor: Float,
    concurrencyLevel: Int,
    mapImpl: (Int, Float) => java.util.Map[K, V])
    extends scala.collection.mutable.Map[K, V]
    with Logging {

  def this(initialCapacity: Int, mapImpl: (Int, Float) => java.util.Map[K, V]) =
    this(initialCapacity, 0.7f, Runtime.getRuntime.availableProcessors() * 2, mapImpl)

  require(initialCapacity >= 0)
  require(initialCapacity <= (1 << 30))
  require(loadFactor > 0.0f && loadFactor < 1.0f)
  require(concurrencyLevel > 0)
  require(concurrencyLevel <= (1 << 30))
  require(mapImpl ne null)

  /** 2<sup>32</sup> &middot; &phi;, &phi; = (&#x221A;5 &minus; 1)/2. */
  private[this] final val INT_PHI = 0x9E3779B9

  private[this] val numSegments: Int = {
    val highBit = java.lang.Integer.highestOneBit(concurrencyLevel)
    if (highBit == concurrencyLevel) concurrencyLevel else highBit << 1
  }

  private[this] val segmentMask: Int = numSegments - 1

  private[this] val segments: Array[java.util.Map[K, V]] = {
    Array.fill(numSegments)(mapImpl(math.max(initialCapacity / numSegments, 1), loadFactor))
  }

  private[this] val segmentLocks: Array[ReentrantReadWriteLock] =
    Array.fill(numSegments)(new ReentrantReadWriteLock())

  private[this] val count = new AtomicInteger()

  override def size: Int = count.get()

  override def empty: scala.collection.mutable.Map[K, V] =
    new ConcurrentSegmentedMap(initialCapacity, loadFactor, numSegments, mapImpl)

  /**
   * Quickly mixes the bits of an integer.
   *
   * This method mixes the bits of the argument by multiplying by the
   * golden ratio and XOR shifting the result. It is borrowed from
   * <a href="https://github.com/OpenHFT/Koloboke">Koloboke</a>, and
   * it has slightly worse behaviour than MurmurHash3 (in open-addressed
   * hash tables the average number of probes is slightly larger),
   * but it's much faster.
   */
  private def segmentIndex(key: K): Int = {
    val h = key.hashCode() * INT_PHI
    (h ^ (h >>> 16)) & segmentMask
  }

  private def find(key: K): V = {
    val segIndex = segmentIndex(key)
    val segment = segments(segIndex)
    val lock = segmentLocks(segIndex).readLock()
    lock.lockInterruptibly()
    try {
      segment.get(key)
    } finally {
      lock.unlock()
    }
  }

  // contains and apply overridden to avoid scala.Option allocations
  override def contains(key: K): Boolean = find(key) != null

  override def apply(key: K): V = {
    val result = find(key)
    if (result != null) result else default(key)
  }

  def get(key: K): Option[V] = Option(find(key))

  override def getOrElseUpdate(key: K, defaultValue: => V): V = {
    val segIndex = segmentIndex(key)
    val segment = segments(segIndex)
    val lock = segmentLocks(segIndex)
    lock.readLock().lockInterruptibly()
    try {
      val result = segment.get(key)
      if (result != null) return result
    } finally {
      lock.readLock().unlock()
    }
    lock.writeLock().lockInterruptibly()
    try {
      val newValue = defaultValue
      val existing = segment.putIfAbsent(key, newValue)
      if (existing != null) newValue else existing
    } finally {
      lock.writeLock().unlock()
    }
  }

  private def replace(key: K, value: V): V = {
    val segIndex = segmentIndex(key)
    val segment = segments(segIndex)
    val lock = segmentLocks(segIndex).writeLock()
    lock.lockInterruptibly()
    try {
      segment.put(key, value)
    } finally {
      lock.unlock()
    }
  }

  override def put(key: K, value: V): Option[V] = Option(replace(key, value))

  override def update(key: K, value: V): Unit = replace(key, value)

  /**
   * Update both key and value in the map with given ones, if the key exists else
   * insert. This is equivalent of: [[remove]]+[[update]] except that the operation
   * is performed atomically.
   */
  def updateEntry(key: K, value: V): Unit = {
    val segIndex = segmentIndex(key)
    val segment = segments(segIndex)
    val lock = segmentLocks(segIndex).writeLock()
    lock.lockInterruptibly()
    try {
      segment.remove(key)
      segment.put(key, value)
    } finally {
      lock.unlock()
    }
  }

  override def +=(kv: (K, V)): this.type = {
    update(kv._1, kv._2)
    this
  }

  private def removeKey(key: K): V = {
    val segIndex = segmentIndex(key)
    val segment = segments(segIndex)
    val lock = segmentLocks(segIndex).writeLock()
    lock.lockInterruptibly()
    try {
      segment.remove(key)
    } finally {
      lock.unlock()
    }
  }

  override def remove(key: K): Option[V] = Option(removeKey(key))

  override def -=(key: K): this.type = {
    removeKey(key)
    this
  }

  override def clear(): Unit = {
    val acquiredLocks = new ArrayBuffer[ReentrantReadWriteLock.WriteLock](numSegments)
    try {
      segmentLocks.foreach { segmentLock =>
        val lock = segmentLock.writeLock()
        lock.lockInterruptibly()
        acquiredLocks += lock
      }
      segments.foreach(_.clear())
    } finally {
      acquiredLocks.foreach { lock =>
        try {
          lock.unlock()
        } catch {
          case t: Throwable => logError("unexpected error in segment write lock release", t)
        }
      }
    }
  }

  override def foreach[U](f: ((K, V)) => U): Unit = {
    var segmentIndex = 0
    while (segmentIndex < numSegments) {
      val segmentIterator = segments(segmentIndex).entrySet().iterator()
      val lock = segmentLocks(segmentIndex).readLock()
      lock.lockInterruptibly()
      try {
        while (segmentIterator.hasNext) {
          val entry = segmentIterator.next()
          f(entry.getKey -> entry.getValue)
        }
        segmentIndex += 1
      } finally {
        lock.unlock()
      }
    }
  }

  private abstract class SegmentMapIterator[U] extends AbstractIterator[U] {

    protected[this] final var segmentIndex: Int = -1
    protected[this] final var entryIndex: Int = _
    protected[this] final var segmentSize: Int = _

    protected def initArrays(size: Int): Unit

    protected def iterateSegment(segmentMap: java.util.Map[K, V]): Unit

    protected def currentEntry(): U

    protected final def initNextSegment(): Unit = {
      segmentIndex += 1
      entryIndex = 0
      segmentSize = 0
      initArrays(-1)
      while (segmentIndex < numSegments) {
        val segmentLock = segmentLocks(segmentIndex).readLock()
        segmentLock.lockInterruptibly()
        try {
          val segmentMap = segments(segmentIndex)
          val size = segmentMap.size()
          if (size > 0) {
            segmentSize = size
            initArrays(size)
            iterateSegment(segmentMap)
            return
          } else {
            segmentIndex += 1
          }
        } finally {
          segmentLock.unlock()
        }
      }
    }

    initNextSegment()

    override final def next(): U = {
      val entry = currentEntry()
      entryIndex += 1
      if (entryIndex >= segmentSize) initNextSegment()
      entry
    }
  }

  override def iterator: Iterator[(K, V)] = new SegmentMapIterator[(K, V)] {

    private[this] var segmentKeys: Array[K] = _
    private[this] var segmentValues: Array[V] = _

    override protected def initArrays(size: Int): Unit = {
      if (size >= 0) {
        segmentKeys = new Array[K](size)
        segmentValues = new Array[V](size)
      } else {
        segmentKeys = null
        segmentValues = null
      }
    }

    override protected def iterateSegment(segmentMap: java.util.Map[K, V]): Unit = {
      var i = 0
      val segmentIterator = segmentMap.entrySet().iterator()
      while (segmentIterator.hasNext) {
        val entry = segmentIterator.next()
        segmentKeys(i) = entry.getKey
        segmentValues(i) = entry.getValue
        i += 1
      }
    }

    override protected def currentEntry(): (K, V) =
      segmentKeys(entryIndex) -> segmentValues(entryIndex)

    override def hasNext: Boolean = segmentKeys ne null
  }

  override def keysIterator: Iterator[K] = new SegmentMapIterator[K] {

    private[this] var segmentKeys: Array[K] = _

    override protected def initArrays(size: Int): Unit = {
      if (size >= 0) {
        segmentKeys = new Array[K](size)
      } else {
        segmentKeys = null
      }
    }

    override protected def iterateSegment(segmentMap: java.util.Map[K, V]): Unit = {
      var i = 0
      val segmentIterator = segmentMap.keySet().iterator()
      while (segmentIterator.hasNext) {
        segmentKeys(i) = segmentIterator.next()
        i += 1
      }
    }

    override protected def currentEntry(): K = segmentKeys(entryIndex)

    override def hasNext: Boolean = segmentKeys ne null
  }

  override def valuesIterator: Iterator[V] = new SegmentMapIterator[V] {

    private[this] var segmentValues: Array[V] = _

    override protected def initArrays(size: Int): Unit = {
      if (size >= 0) {
        segmentValues = new Array[V](size)
      } else {
        segmentValues = null
      }
    }

    override protected def iterateSegment(segmentMap: java.util.Map[K, V]): Unit = {
      var i = 0
      val segmentIterator = segmentMap.values().iterator()
      while (segmentIterator.hasNext) {
        segmentValues(i) = segmentIterator.next()
        i += 1
      }
    }

    override protected def currentEntry(): V = segmentValues(entryIndex)

    override def hasNext: Boolean = segmentValues ne null
  }
}
