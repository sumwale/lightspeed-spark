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

package com.github.spark.lightspeed.util

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.{BiFunction, BiPredicate}

import it.unimi.dsi.fastutil.Hash
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap

/**
 * A thread-safe LRU ordered collection providing some map-like operations. This uses a
 * [[Object2ObjectLinkedOpenHashMap]] to minimize memory object overhead with great performance.
 * Thread-safety is ensured using a read-write lock. All lock acquire operations are interruptable
 * which means that a [[InterruptedException]] is throws if a thread is interrupted while waiting
 * for a read or write lock in a map operation.
 *
 * NOTE: the [[get]] operation does not effect the LRU ordering (unlike the [[put]] operation)
 * and user has to invoke [[getLRU]] if the ordering has to be changed while getting an element.
 *
 * @param initialCapacity The initial capacity of the hash map. For best performance this should
 *                        be close to the maximum number of elements that will be in the map in
 *                        its entire lifetime.
 * @param maximumCapacity The upper cap on the maximum capacity of the hash map. If the size
 *                        exceeds this value, then entries are removed from the map in LRU order.
 * @param loadFactor The load factor for the hash map used for internal sizing of the map which
 *                   should be a floating point number between 0.0 and 1.0. The map will be
 *                   rehashed when the ratio of occupied size to internal size exceeds this value.
 *
 * @tparam K type of keys in the map
 * @tparam V type of values in the map
 */
final class LRUMap[K, V](initialCapacity: Int, val maximumCapacity: Int, loadFactor: Float) {

  def this(maximumCapacity: Int) =
    this(Hash.DEFAULT_INITIAL_SIZE, maximumCapacity, Hash.DEFAULT_LOAD_FACTOR)

  /** the underlying [[Object2ObjectLinkedOpenHashMap]] */
  private[this] val map = new Object2ObjectLinkedOpenHashMap[K, V](initialCapacity, loadFactor)

  /** the read-write lock used for accessing/updating [[map]] */
  private[this] val mapLock = new ReentrantReadWriteLock()

  require(
    maximumCapacity >= initialCapacity,
    s"Expected maximumCapacity = $maximumCapacity to be greater than or equal to $initialCapacity")

  /**
   * Adds a key-value pair to the map overwriting the existing value for the key, if present.
   * The key-value pair is moved to the last position in the iteration order. If the size of map
   * exceeds [[maximumCapacity]], then the least-recently-used key-value is removed from the map.
   *
   * @param k The key to be put in the map. If the key is already present in the map then it
   *          remains unchanged else it is inserted. Null key is allowed.
   * @param v The value to be put in the map. The value either overwrites the one in the map if the
   *          key is already present in the map, or the key-value pair is inserted if not present.
   *          Null value is allowed in which case [[get]] and [[getLRU]] will also return null
   *          result while [[containsKey]] will return true.
   *
   * @return the old value, or null if no value was present for the given key
   */
  def put(k: K, v: V): V = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      val result = map.putAndMoveToLast(k, v)
      if (result == null) expungeIfRequired()
      result
    } finally {
      lock.unlock()
    }
  }

  /**
   * If the size of map exceeds [[maximumCapacity]], then remove entries to bring its size to be
   * less than or equal to [[maximumCapacity]]. Callers must acquire [[mapLock.writeLock]] before
   * invoking this method.
   *
   * @return true if any entries were removed and false otherwise
   */
  private def expungeIfRequired(): Boolean = {
    assert(mapLock.isWriteLockedByCurrentThread)
    if (map.size() > maximumCapacity) {
      val iter = map.keySet().iterator()
      while (iter.hasNext) {
        iter.remove()
        if (map.size() <= maximumCapacity) return true
      }
      true
    } else false
  }

  /**
   * If the key-value pair is present for the specified key, then attempts to compute a new
   * mapping given the key and its current mapped value. The key-value pair is moved to the last
   * position in the iteration order. The entire method invocation is performed atomically and
   * will block other concurrent update operations on this map by other threads, so the computation
   * should be short.
   *
   * @param k The key to be updated in the map. If the key is already present in the map and
   *          associated with a non-null value, then `remappingFunction` is applied to the mapped
   *          value and the new value is put into the map. Null key is allowed.
   * @param remappingFunction function to compute the new value given the current key-value pair
   *
   * @return the new value associated with the given key, or null if the key was absent from the
   *         map or associated with a null value
   */
  def update(k: K, remappingFunction: BiFunction[_ >: K, _ >: V, _ <: V]): V = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      val oldValue = map.get(k)
      if (oldValue != null) {
        val result = remappingFunction.apply(k, oldValue)
        map.putAndMoveToLast(k, result)
        result
      } else oldValue
    } finally {
      lock.unlock()
    }
  }

  /**
   * Returns the value to which the given key is mapped or null if no value is mapped.
   *
   * @param k the key to lookup
   *
   * @return the value mapped for the key, or null if no value was present for the given key
   */
  def get(k: K): V = {
    val lock = mapLock.readLock()
    lock.lockInterruptibly()
    try {
      map.get(k)
    } finally {
      lock.unlock()
    }
  }

  /**
   * Returns the value to which the given key is mapped or null if no value is mapped.
   * If the key is present, it is moved to the last position in the iteration order.
   *
   * @param k the key to lookup
   *
   * @return the value mapped for the key, or null if no value was present for the given key
   */
  def getLRU(k: K): V = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      map.getAndMoveToLast(k)
    } finally {
      lock.unlock()
    }
  }

  /**
   * Returns true if this to which the given key is mapped or null if no value is mapped.
   *
   * @param k the key to lookup
   *
   * @return the value mapped for the key, or null if no value was present for the given key
   */
  def containsKey(k: K): Boolean = {
    val lock = mapLock.readLock()
    lock.lockInterruptibly()
    try {
      map.containsKey(k)
    } finally {
      lock.unlock()
    }
  }

  /**
   * Returns the number of key-value pairs in this map.
   */
  def size(): Int = {
    val lock = mapLock.readLock()
    lock.lockInterruptibly()
    try {
      map.size()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Removes the key-value pair for a given key if present.
   *
   * @param k the key of the key-value pair to be removed
   *
   * @return the old value, or null if no value was present for the given key
   */
  def remove(k: K): V = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      map.remove(k)
    } finally {
      lock.unlock()
    }
  }

  /**
   * Removes the key-value pairs for the given collection of keys.
   *
   * @param keys the collection of keys to be removed
   */
  def removeAllKeys(keys: java.util.Collection[K]): Unit = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      if (keys.size() >= map.size() && keys.isInstanceOf[java.util.Set[_]]) {
        val iter = map.keySet().iterator()
        while (iter.hasNext) {
          if (keys.contains(iter.next())) iter.remove()
        }
      } else {
        val iter = keys.iterator()
        while (iter.hasNext) map.remove(iter.next())
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * Remove all key-value pairs matching a given predicate.
   *
   * @param predicate The [[BiPredicate]] to be evaluated for the key-value pairs. If this returns
   *                  true then the pair will be removed from the map.
   */
  def removeAll(predicate: BiPredicate[K, V]): Int = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      var numRemoved = 0
      val iter = map.object2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        if (predicate.test(entry.getKey, entry.getValue)) {
          iter.remove()
          numRemoved += 1
        }
      }
      numRemoved
    } finally {
      lock.unlock()
    }
  }

  /**
   * Iterate the map in LRU order applying a predicate to all the key-value pairs in the map.
   * The iteration is stopped if the given predicate evaluates to false. The predicate cannot
   * perform any write operation on the map itself else it will result in a deadlock.
   *
   * @param predicate The [[BiPredicate]] to be evaluated for the key-value pairs. If this returns
   *                  false then the iteration will be terminated.
   */
  def foreach(predicate: BiPredicate[K, V]): Unit = {
    val lock = mapLock.readLock()
    lock.lockInterruptibly()
    try {
      val iter = map.object2ObjectEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        if (!predicate.test(entry.getKey, entry.getValue)) return
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * If the size of map exceeds [[maximumCapacity]], then remove entries to bring its size to be
   * less than or equal to [[maximumCapacity]].
   *
   * @return true if any entries were removed and false otherwise
   */
  def shrinkToMaxCapacity(): Boolean = {
    val lock = mapLock.writeLock()
    lock.lockInterruptibly()
    try {
      expungeIfRequired()
    } finally {
      lock.unlock()
    }
  }
}
