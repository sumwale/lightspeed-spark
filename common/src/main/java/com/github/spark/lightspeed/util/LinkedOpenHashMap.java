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

package com.github.spark.lightspeed.util;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;

/**
 * Extension to {@link Object2ObjectLinkedOpenHashMap} to ensure that put operation
 * moves the entry to the end of list in the case of overwrite too so follows LRU
 * order in terms of puts.
 */
public final class LinkedOpenHashMap<K, V> extends Object2ObjectLinkedOpenHashMap<K, V> {

  public LinkedOpenHashMap() {
    super();
  }

  public LinkedOpenHashMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  @Override
  public V put(K k, V v) {
    return putAndMoveToLast(k, v);
  }
}
