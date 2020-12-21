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

import io.spark.lightspeed.memory.EvictionManager

/**
 * Bare statistics of `CacheValue`s used by [[EvictionManager]] to record for future usage after
 * eviction when the object might be `resurrected`.
 */
sealed class CacheValueStats(
    val weightage: Double,
    val generation: Int,
    val generationalBoost: Double) {

  /**
   * True if stats for compressed object else false.
   */
  def isCompressed: Boolean = false

  /**
   * The [[CompressedCacheObject.compressionSavings]] field to record any additional savings due
   * to compression.
   */
  def compressionSavings: Double =
    throw new UnsupportedOperationException("compressionSavings: decompressed object")
}

/**
 * Additional `compressionSavings` stat for compressed objects.
 */
final class CompressedCacheValueStats(
    weightage: Double,
    generation: Int,
    generationalBoost: Double,
    override val compressionSavings: Double)
    extends CacheValueStats(weightage, generation, generationalBoost) {

  override def isCompressed: Boolean = true
}
