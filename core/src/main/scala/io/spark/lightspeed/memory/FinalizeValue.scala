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

package io.spark.lightspeed.memory

import com.google.common.base.FinalizableWeakReference

/**
 * Base class for finalization of [[CacheValue]] that users must implement for objects that
 * need additional cleanup after eviction (e.g. freeing memory for off-heap objects).
 * The same must be returned by the [[TransformValue.createFinalizer]] method given an object.
 *
 * Note: Implementations should normally not require to override the [[clear]] method but if they
 * do, then it must invoke `super.clear()` in the overridden method unless the implementation
 * provides its own way of invoking [[EvictionService.removeWeakReference]].
 */
abstract class FinalizeValue[T <: CacheValue](value: T)
    extends FinalizableWeakReference[T](value, EvictionService.finalizerQueue) {

  /**
   * Base class clear of the contained referent. Provided as a convenience if an implementation
   * intends to override [[clear]] method without invoking `super.clear()` (in which case it
   * should have its own way of invoking [[EvictionService.removeWeakReference]]).
   */
  final def baseClear(): Unit = super.clear()

  override def clear(): Unit = {
    val value = super.get()
    if (value ne null) {
      EvictionService.removeWeakReference(this)
    }
    baseClear()
  }
}
