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

import java.util.concurrent.ConcurrentHashMap

import scala.reflect.{classTag, ClassTag}

import com.github.spark.lightspeed.memory.internal.AdaptiveEvictionManager
import com.github.spark.lightspeed.util.Collections
import com.google.common.base.FinalizableReferenceQueue

/**
 * Creates and maintains [[EvictionManager]] implementations for different types of [[CacheValue]]s
 * possibly differing in heap vs off-heap memory, or other differences. It is also possible to
 * create a global [[EvictionManager]] for just the base [[CacheValue]] type.
 */
object EvictionService {

  private final val evictionManagers =
    new ConcurrentHashMap[Class[_], EvictionManager[_ <: CacheValue, _ <: CacheValue]]()

  /**
   * The pending `WeakReference`s for objects that remain to be `finalized` are placed here
   * and removed by the `clear` method of the [[FinalizeValue]].
   *
   * Uses a synchronized OpenHashSet instead of a ConcurrentHashMap to reduce overhead.
   */
  private[this] val weakReferences =
    java.util.Collections
      .synchronizedSet(Collections.newHashSet[FinalizeValue[_]]())

  lazy val finalizerQueue = new FinalizableReferenceQueue

  private def getOrCreateAdaptiveEvictor(
      compressedType: Class[_],
      decompressedType: Class[_],
      createAdaptiveEvictor: () => AdaptiveEvictionManager[_ <: CacheValue, _ <: CacheValue])
    : EvictionManager[_ <: CacheValue, _ <: CacheValue] = {
    // mapping is maintained against the both types but a single EvictionManager instance
    val evictionManager = evictionManagers
      .computeIfAbsent(compressedType, _ => createAdaptiveEvictor())
    evictionManagers
      .computeIfAbsent(decompressedType, _ => evictionManager)
  }

  def getOrCreateAdaptiveEvictor[C <: CacheValue: ClassTag, D <: CacheValue: ClassTag](
      maxMemorySize: Long,
      millisForTwiceFreq: Long,
      decompressionToDiskReadCost: Double,
      maxMemoryOverflowBeforeEvict: Double,
      maxEvictedEntries: Int): EvictionManager[C, D] = {
    getOrCreateAdaptiveEvictor(
      classTag[C].runtimeClass,
      classTag[D].runtimeClass,
      () =>
        new AdaptiveEvictionManager[C, D](
          maxMemorySize,
          millisForTwiceFreq,
          decompressionToDiskReadCost,
          maxMemoryOverflowBeforeEvict,
          maxEvictedEntries)).asInstanceOf[EvictionManager[C, D]]
  }

  def getOrCreateAdaptiveEvictor[C <: CacheValue: ClassTag, D <: CacheValue: ClassTag](
      maxMemorySize: Long,
      millisForTwiceFreq: Long): EvictionManager[C, D] = {
    getOrCreateAdaptiveEvictor(
      classTag[C].runtimeClass,
      classTag[D].runtimeClass,
      () => new AdaptiveEvictionManager[C, D](maxMemorySize, millisForTwiceFreq))
      .asInstanceOf[EvictionManager[C, D]]
  }

  /**
   * Search for adaptive [[EvictionManager]] for given class or its super class hierarchy.
   */
  def getAdaptiveEvictorForClass(
      valueType: Class[_]): Option[EvictionManager[_ <: CacheValue, _ <: CacheValue]] = {
    var lookupType = valueType
    while (lookupType ne null) {
      val evictionManager = evictionManagers.get(lookupType)
      if (evictionManager ne null) return Some(evictionManager)
      lookupType = lookupType.getSuperclass
    }
    None
  }

  private[memory] def addWeakReference(ref: FinalizeValue[_]): Boolean = {
    weakReferences.add(ref)
  }

  private[memory] def removeWeakReference(ref: FinalizeValue[_]): Boolean = {
    weakReferences.remove(ref)
  }
}
