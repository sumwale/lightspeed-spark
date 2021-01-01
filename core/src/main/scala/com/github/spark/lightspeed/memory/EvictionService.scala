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

  /**
   * The global set of [[EvictionManager]] instances.
   */
  private[this] final val evictionManagers =
    new ConcurrentHashMap[Class[_], (EvictionManager[_ <: CacheValue, _ <: CacheValue], Class[_])]

  /**
   * The pending `WeakReference`s for objects that remain to be `finalized` are placed here
   * and removed by the `clear` method of the [[FinalizeValue]].
   *
   * Uses synchronization on an OpenHashSet instead of a ConcurrentHashMap to reduce overhead.
   */
  private[this] final val weakReferences = Collections.newOpenHashSet[FinalizeValue[_]]()

  /**
   * Global `ReferenceQueue` used for all `WeakReference`s maintained by the [[EvictionService]].
   * This is `ReferenceQueue` is cleaned up by a background thread. This is public so that other
   * components that need to cleanup stuff using `Reference`s can also make use of the same
   * `ReferenceQueue` instead of adding their own queues and cleanup mechanisms.
   */
  lazy val finalizerQueue = new FinalizableReferenceQueue

  /**
   * Get or create adaptive [[EvictionManager]]'s for given compressed and decompressed classes.
   */
  private def getOrCreateAdaptiveEvictor(
      compressedType: Class[_],
      decompressedType: Class[_],
      createAdaptiveEvictor: () => AdaptiveEvictionManager[_ <: CacheValue, _ <: CacheValue])
    : EvictionManager[_ <: CacheValue, _ <: CacheValue] = {
    // mapping is maintained against the both types but a single EvictionManager instance
    var evictionManager = evictionManagers.get(compressedType)._1
    if (evictionManager eq null) {
      evictionManager = evictionManagers
        .computeIfAbsent(compressedType, _ => createAdaptiveEvictor() -> decompressedType)
        ._1
      if (compressedType != decompressedType) {
        evictionManagers.put(decompressedType, evictionManager -> compressedType)
      }
    }
    evictionManager
  }

  /**
   * Get or create a new adaptive [[EvictionManager]] for given compressed/decompressed
   * [[ClassValue]] implementations expressed as [[ClassTag]]s for the type parameters.
   * The two can be same if there are no separate compressed/decompressed versions.
   *
   * @param maxMemorySize the initial maximum memory limit
   * @param millisForTwiceFreq milliseconds after which the timestamp will have same weightage
   *                           as two accesses as of now; this should not be smaller than 10 secs
   *                           and recommended to be 5 minutes or more else the exponent required
   *                           for weightage becomes too large
   * @param decompressionToDiskReadCost ratio of cost to decompress a disk block to reading it
   *                                    from disk (without OS caching)
   * @param maxMemoryOverflowBeforeEvict maximum ratio of `maxMemorySize` that can overflow before
   *                                     eviction would forcefully block putter/getter threads
   * @param maxEvictedEntries maximum count of old evicted entries before all their stats are purged
   *
   * @tparam C the type of compressed objects
   * @tparam D the type of decompressed objects
   *
   * @return an existing adaptive [[EvictionManager]] for given classes or return a new one
   */
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

  /**
   * Get or create a new adaptive [[EvictionManager]] for given compressed/decompressed
   * [[ClassValue]] implementations expressed as [[ClassTag]]s for the type parameters.
   * The two can be same if there are no separate compressed/decompressed versions.
   *
   * @param maxMemorySize the initial maximum memory limit
   * @param millisForTwiceFreq milliseconds after which the timestamp will have same weightage
   *                           as two accesses as of now; this should not be smaller than 10 secs
   *                           and recommended to be 5 minutes or more else the exponent required
   *                           for weightage becomes too large
   *
   * @tparam C the type of compressed objects
   * @tparam D the type of decompressed objects
   *
   * @return an existing adaptive [[EvictionManager]] for given classes or return a new one
   */
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
      if (evictionManager ne null) return Some(evictionManager._1)
      lookupType = lookupType.getSuperclass
    }
    None
  }

  /**
   * Remove all [[EvictionManager]]s against classes matching the given predicate and clear
   * all entries in the removed [[EvictionManager]]s.
   *
   * @param predicate should return true for a class if [[EvictionManager]] for that class should
   *                  be removed and false otherwise
   */
  def removeEvictionManagers(predicate: Class[_] => Boolean): Unit = {
    val clearAll = (_: Comparable[_ <: AnyRef]) => true
    val managerIter = evictionManagers.entrySet().iterator()
    val classesToBeRemoved = Collections.newOpenHashSet[Class[_]]()
    while (managerIter.hasNext) {
      val entry = managerIter.next()
      val removeClass = entry.getKey
      if (predicate(removeClass)) {
        // note the associated decompressed/compressed class for removal
        val otherRemoveClass = entry.getValue._2
        if (otherRemoveClass != removeClass) classesToBeRemoved.add(otherRemoveClass)
        classesToBeRemoved.remove(removeClass)
        managerIter.remove()
        entry.getValue._1.removeAll(clearAll)
      }
    }
    if (!classesToBeRemoved.isEmpty) {
      val removeIter = classesToBeRemoved.iterator()
      while (removeIter.hasNext) {
        val entry = evictionManagers.remove(removeIter.next())
        if (entry ne null) entry._1.removeAll(clearAll)
      }
    }
  }

  /**
   * Add a [[FinalizeValue]] extension of `WeakReference` to a global set. Should only be used
   * internally by [[EvictionManager]] when a `finalizer` is added to [[CacheValue]] for global
   * bookkeeping so that a `WeakReference` is retained for the [[CacheValue]] before being removed
   * by explicit `release` or after being collected by GC into [[finalizerQueue]].
   *
   * @return true if the instance was added and false if the same instance already exists
   */
  private[memory] def addWeakReference(ref: FinalizeValue[_]): Boolean = {
    weakReferences.synchronized(weakReferences.add(ref))
  }

  /**
   * Remove [[FinalizeValue]] added by a previous call to [[addWeakReference]]. Used internally
   * by [[FinalizeValue.clear]] and shouldn't be called otherwise.
   *
   * @return true if the instance was removed and false if the instance did not exist in the
   *         global set
   */
  private[memory] def removeWeakReference(ref: FinalizeValue[_]): Boolean = {
    weakReferences.synchronized(weakReferences.remove(ref))
  }
}
