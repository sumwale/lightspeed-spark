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
 * The implementation must be returned by [[TransformValue.createFinalizer]] given the
 * [[CacheValue]] object. The [[FreeValue.free]] method of this class should be identical to that
 * of the [[CacheValue]] itself. Typically users should implement a common trait extending
 * [[FreeValue]] having a concrete implementation of [[FreeValue.free]] which can then be used by
 * both [[CacheValue]] and [[FinalizeValue]] implementations.
 *
 * Note: Implementations cannot override the [[finalizeReferent]] method and should rather override
 * `free` for any required finalization actions. The invocation of `free` is ensured to be done
 * exactly once so implementations can safely invoke actions that may otherwise cause trouble if
 * done twice (e.g. freeing off-heap memory).
 */
abstract class FinalizeValue[T <: CacheValue](value: T)
    extends FinalizableWeakReference[T](value, EvictionService.finalizerQueue)
    with FreeValue {

  /**
   * Flag to ensure that [[free]] is invoked no more than once.
   */
  @volatile protected[this] final var invoked: Int = 0

  override final def finalizeReferent(): Unit = {
    if (Utils.compareAndSetInvoked(this, 0, 1)) {
      EvictionService.removeWeakReference(this)
      free()
    }
  }
}
