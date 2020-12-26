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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet

/**
 * Set of utility methods around useful fastutil collections while also creating a minimal shadow
 * jar for the module to include only the required fastutil classes to reduce the size of jars.
 */
object Collections {

  /**
   * Returns an open-addressed HashMap. Primary advantage is much reduced overhead compared
   * to normal java/scala HashMap implementations.
   *
   * @tparam K type of keys in the set
   */
  def newOpenHashSet[K](): JSet[K] = new ObjectOpenHashSet[K]

  /**
   * Returns an open-addressed HashMap. Primary advantage is much reduced overhead compared
   * to normal java/scala HashMap implementations.
   *
   * @param initialCapacity hint for minimum number of elements in the set
   * @param loadFactor the load factor of the hash set
   *
   * @tparam K type of keys in the set
   */
  def newOpenHashSet[K](initialCapacity: Int, loadFactor: Float): JSet[K] =
    new ObjectOpenHashSet[K](initialCapacity, loadFactor)

  /**
   * Returns an open-addressed HashMap which is LRU in terms of puts but not reads. Primary
   * advantage is much reduced overhead compared to normal linked/LRU map implementations.
   *
   * @tparam K type of keys in the map
   * @tparam V type of values in the map
   */
  def newLRUHashMap[K, V](): JMap[K, V] = new PutLRUHashMap[K, V]()

  /**
   * Returns an open-addressed HashMap which is LRU in terms of puts but not reads. Primary
   * advantage is much reduced overhead compared to normal linked/LRU map implementations.
   *
   * @param initialCapacity hint for minimum number of elements in the map
   * @param loadFactor the load factor of the hash map
   *
   * @tparam K type of keys in the map
   * @tparam V type of values in the map
   */
  def newLRUHashMap[K, V](initialCapacity: Int, loadFactor: Float): JMap[K, V] =
    new PutLRUHashMap[K, V](initialCapacity, loadFactor)
}
