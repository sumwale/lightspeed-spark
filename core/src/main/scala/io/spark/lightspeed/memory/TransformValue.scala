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

/**
 * Interface to compress/decompress [[CacheValue]]s and `finalize` them for release. This is kept
 * as a separate interface from [[CacheValue]] since a reference to this can be held by
 * [[EvictionManager]] internally even after a [[CacheValue]] has been evicted. Implementations
 * should try not to create an object for every [[CacheValue]] rather should have static
 * implementations (e.g. per `compressionAlgorithm`) to minimize the objects held in cache.
 * However, if `finalize` requires more information beyond [[CacheValue.finalizationInfo]] and
 * other arguments provided, then this needs to be created for every [[CacheValue]] instance
 * to include that information.
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
   * Any finalization actions to be taken for a cached object. This method should
   * be able to deal with both compressed and decompressed versions of the [[CacheValue]].
   * Typically this will release any off-heap memory used by the [[CacheValue]].
   *
   * This should be identical to the object's [[CacheValue.free]] method.
   */
  def finalize(key: Comparable[AnyRef], isCompressed: Boolean, finalizationInfo: Long): Unit
}
