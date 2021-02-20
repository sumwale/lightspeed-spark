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

/**
 * Interface to compress/decompress [[CacheValue]]s and create [[FinalizeValue]] for their release.
 * This is kept as a separate interface from [[CacheValue]] so it can be a completely separate
 * object or can be implemented by [[CacheValue]] itself. For former case, implementations
 * should try not to create an object for every [[CacheValue]] rather should have static
 * implementations (e.g. per `compressionAlgorithm`) to minimize the objects held in cache.
 *
 * @tparam C the type of compressed objects
 * @tparam D the type of decompressed objects
 */
trait TransformValue[C <: CacheValue, D <: CacheValue] {

  /**
   * The compression algorithm used by this implementation. If [[None]], then no
   * compression/decompression is performed for the object.
   */
  def compressionAlgorithm: Option[String]

  /**
   * Compress the given value object.
   */
  def compress(value: D): C

  /**
   * Get the size in bytes of the compressed version of the given object without actually
   * performing the compression.
   */
  def compressedSize(value: D): Long

  /**
   * Decompress the given value object.
   */
  def decompress(value: C): D

  /**
   * Get the size in bytes of the decompressed version of the given object without actually
   * performing the decompression.
   */
  def decompressedSize(value: C): Long

  /**
   * Create a [[FinalizeValue]] instance for given object that will perform any finalization
   * actions to be taken for a cached object. Typically this will release any off-heap memory
   * used by the [[CacheValue]] and/or perform bookkeeping in memory managers like that of Spark.
   *
   * @return Optionally return [[FinalizeValue]] instance for the given [[CacheValue]]
   *         or [[None]] if no finalization is required for the object
   */
  protected def createFinalizer[T <: CacheValue](value: T): Option[FinalizeValue[T]]

  /**
   * Set the `finalizer` field of [[CacheValue]] if empty using [[createFinalizer]] and if created,
   * put in [[EvictionService]]'s map to maintain the `WeakReference`.
   */
  final def addFinalizerIfMissing[T <: CacheValue](value: T): Unit = {
    if (value.finalizer eq null) {
      createFinalizer(value) match {
        case Some(f) =>
          value.finalizer = f
          // maintain in a separate map for pending WeakReferences that will be cleared when the
          // underlying CacheValue is finalized
          val finalizer = value.finalizer
          if (finalizer ne null) EvictionService.addWeakReference(finalizer)
        case _ =>
      }
    }
  }
}
