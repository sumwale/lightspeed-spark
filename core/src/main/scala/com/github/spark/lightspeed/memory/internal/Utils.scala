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

package com.github.spark.lightspeed.memory.internal

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

import com.github.spark.lightspeed.memory.{CacheValue, EvictionManager, FinalizeValue, TransformValue}

/**
 * Some common utility methods.
 */
object Utils {

  /**
   * Atomic updater for `invoked` field of [[FinalizeValue]] class.
   */
  private[this] final val invokedUpdater: AtomicIntegerFieldUpdater[FinalizeValue[_]] =
    AtomicIntegerFieldUpdater.newUpdater[FinalizeValue[_]](classOf[FinalizeValue[_]], "invoked")

  /**
   * Atomically set the `invoked` field of a [[FinalizeValue]] object to `update` if current
   * value is `expect`.
   *
   * @param f the [[FinalizeValue]] instance whose field to conditionally set
   * @param expect the expected value
   * @param update the new value
   *
   * @return true if the update was successful and false otherwise
   */
  def compareAndSetInvoked(
      f: FinalizeValue[_ <: CacheValue],
      expect: Int,
      update: Int): Boolean = {
    invokedUpdater.compareAndSet(f, expect, update)
  }

  /**
   * Create an appropriate [[StoredCacheObject]] for given [[CacheValue]] and [[TransformValue]].
   * This should only be used by [[EvictionManager]] to create objects for storage in the cache.
   *
   * @param key A unique key for the object. This object cannot be same as the value itself or
   *            contain a reference to value since this can be retained for long-term statistics.
   * @param value The object to be put in the [[EvictionManager]] cache.
   * @param transformer Implementation to compress/decompress the provided `value` and create
   *                    `finalizer` for release. Typically this should have static implementations
   *                    (e.g. per `compressionAlgorithm`) to minimize the objects held in cache.
   * @param timeWeightage Overall weightage calculated for `timestamp` provided to
   *                      [[EvictionManager]] API methods.
   * @param decompressionToDiskReadCost Ratio of cost to decompress a disk block to reading it
   *                                    from disk (without OS caching).
   *
   * @tparam C the type of compressed objects
   * @tparam D the type of decompressed objects
   *
   * @return a [[StoredCacheObject]] to be cached by [[EvictionManager]]
   */
  def newStoredCacheObject[C <: CacheValue, D <: CacheValue](
      key: Comparable[_ <: AnyRef],
      value: CacheValue,
      transformer: TransformValue[C, D],
      timeWeightage: Double,
      decompressionToDiskReadCost: Double): StoredCacheObject[_ <: CacheValue] = {
    if (value.isCompressed) {
      val compressedVal = value.asInstanceOf[C]
      val compressionSavings = calcCompressionSavings(
        timeWeightage,
        decompressionToDiskReadCost,
        value.memorySize,
        transformer.decompressedSize(compressedVal))
      new CompressedCacheObject[C, D](
        key,
        compressedVal,
        transformer,
        timeWeightage + compressionSavings,
        compressionSavings)
    } else {
      new DecompressedCacheObject[D, C](key, value.asInstanceOf[D], transformer, timeWeightage)
    }
  }

  /**
   * Compression of an object can provide considerable savings in memory. However, it also
   * has the overhead of decompression. This is a quick way to determine what is better using
   * the provided `decompressionToDiskReadCost` that provides ratio of cost of decompressing
   * blocks to reading those from disk.
   *
   * @param timeWeightage Overall weightage calculated for `timestamp` provided to
   *                      [[EvictionManager]] API methods.
   * @param decompressionToDiskReadCost Ratio of cost to decompress a disk block to reading it
   *                                    from disk (without OS caching)
   * @param compressedSize Size in bytes ([[CacheValue.memorySize]]) of the compressed value.
   * @param decompressedSize Size in bytes ([[CacheValue.memorySize]]) of the decompressed value.
   *
   * @return estimated additional weightage to use for savings due to compression
   */
  def calcCompressionSavings(
      timeWeightage: Double,
      decompressionToDiskReadCost: Double,
      compressedSize: Long,
      decompressedSize: Long): Double = {
    val compressionSavingsFactor = ((decompressedSize.toDouble / compressedSize.toDouble) *
      (1.0 - decompressionToDiskReadCost)) - 1.0
    if (compressionSavingsFactor > 0.0) timeWeightage * compressionSavingsFactor else 0.0
  }
}
