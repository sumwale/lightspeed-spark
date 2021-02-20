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

import java.util.{Map => JMap, Set => JSet}

import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectOpenHashSet}

/**
 * Set of utility methods around useful fastutil collections while also creating a minimal shadow
 * jar for the module to include only the required fastutil classes to reduce the size of jars.
 */
object Collections {

  /**
   * Returns an open-addressed HashSet. Primary advantage is much reduced overhead compared
   * to normal java/scala HashSet implementations.
   *
   * @tparam K type of keys in the set
   */
  def newOpenHashSet[K](): JSet[K] = new ObjectOpenHashSet[K]

  /**
   * Returns an open-addressed HashSet. Primary advantage is much reduced overhead compared
   * to normal java/scala HashSet implementations.
   *
   * @param initialCapacity hint for minimum number of elements in the set
   * @param loadFactor the load factor of the hash set
   *
   * @tparam K type of keys in the set
   */
  def newOpenHashSet[K](initialCapacity: Int, loadFactor: Float): JSet[K] =
    new ObjectOpenHashSet[K](initialCapacity, loadFactor)

  /**
   * Returns an open-addressed HashMap. Primary advantage is much reduced overhead compared
   * to normal java/scala HashMap implementations.
   *
   * @tparam K type of keys in the map
   * @tparam V type of values in the map
   */
  def newOpenHashMap[K, V](): JMap[K, V] = new Object2ObjectOpenHashMap[K, V]

  /**
   * Returns an open-addressed HashMap. Primary advantage is much reduced overhead compared
   * to normal java/scala HashMap implementations.
   *
   * @param initialCapacity hint for minimum number of elements in the map
   * @param loadFactor the load factor of the hash map
   *
   * @tparam K type of keys in the map
   * @tparam V type of values in the map
   */
  def newOpenHashMap[K, V](initialCapacity: Int, loadFactor: Float): JMap[K, V] =
    new Object2ObjectOpenHashMap[K, V](initialCapacity, loadFactor)

  /**
   * Returns an [[LRUMap]] with given parameters. Primary advantage is much reduced overhead
   * compared to normal linked/LRU map implementations.
   *
   * @param maximumCapacity The upper cap on the maximum capacity of the hash map. If the size
   *                        exceeds this value, then entries are removed from the map in LRU order.
   *
   * @tparam K type of keys in the map
   * @tparam V type of values in the map
   */
  def newLRUMap[K, V](maximumCapacity: Int): LRUMap[K, V] = new LRUMap[K, V](maximumCapacity)

  /**
   * Returns an [[LRUMap]] with given parameters. Primary advantage is much reduced overhead
   * compared to normal linked/LRU map implementations.
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
  def newLRUMap[K, V](
      initialCapacity: Int,
      maximumCapacity: Int,
      loadFactor: Float): LRUMap[K, V] =
    new LRUMap[K, V](initialCapacity, maximumCapacity, loadFactor)
}
