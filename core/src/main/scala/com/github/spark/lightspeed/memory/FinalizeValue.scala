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

package com.github.spark.lightspeed.memory

import com.github.spark.lightspeed.memory.internal.Utils
import com.google.common.base.FinalizableWeakReference

/**
 * Base class for finalization of [[CacheValue]] that users must implement for objects that
 * need additional cleanup after eviction (e.g. freeing memory for off-heap objects).
 * The implementation must be returned by [[TransformValue.createFinalizer]] given the object.
 *
 * Note: Implementations cannot override the [[clear]] method and rather should override
 * [[basicClear]] for any additional cleanup actions (which should also call `super.basicClear`).
 * The invocation of [[basicClear]] is ensured to be done exactly once.
 */
abstract class FinalizeValue[T <: CacheValue](value: T)
    extends FinalizableWeakReference[T](value, EvictionService.finalizerQueue) {

  @volatile protected[this] final var invoked: Int = 0

  /**
   * Actions required to `clear` this `WeakReference` apart from removing from the
   * [[EvictionService]]'s `WeakReference` map. This can be invoked independently if
   * it is known that this finalizer has not been added to [[EvictionService]]'s map.
   *
   * Implementations should override this method to add any actions required in `clear`
   * just prior to [[finalizeReferent]] such as accounting in Spark's MemoryManager.
   */
  def basicClear(): Unit = super.clear()

  override final def clear(): Unit = {
    if (Utils.compareAndSetInvoked(this, 0, 1)) {
      EvictionService.removeWeakReference(this)
      basicClear()
    }
  }
}
