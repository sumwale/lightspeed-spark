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

package com.github.spark.lightspeed.collection

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, CyclicBarrier}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.github.spark.lightspeed.SparkSuite
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectOpenHashSet}
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import org.apache.spark.benchmark.SparkBenchmark

/**
 * Performance and memory overhead comparison of JDK [[ConcurrentHashMap]], and
 * [[ConcurrentSegmentedMap]] with various contained open hash maps including from
 * eclipse-collection, fastutil and Spark.
 */
class ConcurrentMapTest extends SparkSuite {

  private def newFastutilMap(
      initialCapacity: Int,
      loadFactor: Float): java.util.Map[String, String] = {
    new Object2ObjectOpenHashMap[String, String](initialCapacity, loadFactor)
  }

  private def newEclipseMap(
      initialCapacity: Int,
      loadFactor: Float): java.util.Map[String, String] = {
    new UnifiedMap[String, String](initialCapacity, loadFactor)
  }

  // noinspection ScalaUnusedSymbol
  private def newScalaMap(
      initialCapacity: Int,
      loadFactor: Float): java.util.Map[String, String] = {
    new mutable.OpenHashMap[String, String](initialCapacity).asJava
  }

  @inline private def newKey(i: Int): String = s"stringKeyForHashMapTesting_$i"

  @inline private def newValue(i: Int): String = s"stringValueForHashMapTesting_$i"

  private def populateMap(map: mutable.Map[String, String], numItems: Int): Unit = {
    require(numItems > 0)

    var i = 0
    while (i < numItems) {
      map.update(newKey(i), newValue(i))
      i += 1
    }
  }

  private def runGets(
      map: mutable.Map[String, String],
      numOperations: Int,
      numItems: Int): Unit = {
    require(numOperations > 0)
    require(numItems > 0)

    val random = new java.util.Random()
    val blockSize = 100
    require(numOperations > blockSize)
    var i = 0
    while (i < numOperations) {
      val start = random.nextInt(numItems)
      var j = 0
      while (j < blockSize) {
        val keyIndex = (start + j) % numItems
        assert(map(newKey(keyIndex)) === newValue(keyIndex))
        j += 1
        i += 1
      }
    }
  }

  private def runPuts(
      map: mutable.Map[String, String],
      numOperations: Int,
      numItems: Int): Unit = {
    require(numOperations > 0)
    require(numItems > 0)

    val random = new java.util.Random()
    val blockSize = 100
    require(numOperations > blockSize)
    var i = 0
    while (i < numOperations) {
      val start = random.nextInt(numItems)
      var j = 0
      while (j < blockSize) {
        val keyIndex = (start + j) % numItems
        val v = newValue(keyIndex)
        assert(map.put(newKey(keyIndex), v) === Some(v))
        j += 1
        i += 1
      }
    }
  }

  private def runMixed(
      map: mutable.Map[String, String],
      numOperations: Int,
      numItems: Int,
      putRatio: Double): Unit = {
    require(numOperations > 0)
    require(numItems > 0)
    require(putRatio > 0.0 && putRatio < 1.0)

    val random = new java.util.Random()
    val blockSize = 100
    require(numOperations > blockSize)
    val putBlockSize = (blockSize.toDouble * putRatio).toInt
    val getBlockSize = blockSize - putBlockSize
    var i = 0
    while (i < numOperations) {
      val start = random.nextInt(numItems)
      var j = 0
      while (j < putBlockSize) {
        val keyIndex = (start + j) % numItems
        val v = newValue(keyIndex)
        assert(map.put(newKey(keyIndex), v) === Some(v))
        j += 1
        i += 1
      }
      j = 0
      while (j < getBlockSize) {
        val keyIndex = (start + j) % numItems
        assert(map(newKey(keyIndex)) === newValue(keyIndex))
        j += 1
        i += 1
      }
    }
  }

  private def runAction(
      action: () => Unit,
      opType: String,
      threadId: Int,
      barrier: CyclicBarrier): Unit = {
    try {
      barrier.await()
      action()
    } catch {
      case t: Throwable =>
        logError(s"Unexpected failure in operation [$opType] for thread $threadId", t)
    }
  }

  private def runThreaded(numThreads: Int, action: () => Unit, opType: String): Unit = {
    require(numThreads > 0)
    val barrier = new CyclicBarrier(numThreads)
    if (numThreads == 1) runAction(action, opType, threadId = 0, barrier)
    else {
      val threads =
        (0 until numThreads).map(i => new Thread(() => runAction(action, opType, i, barrier)))
      threads.foreach(_.start())
      threads.foreach(_.join())
    }
  }

  benchmark("Benchmark concurrent map operations") {
    val allNumItems = Array(1000, 10000, 100000, 1000000)
    val allNumThreads = Array(1, 4, 8, 16)
    val putRatios = Array(0.1, 0.5, 0.8)
    val concurrentMapTypes = Array("JDK CHM" -> { c: Int =>
      new ConcurrentHashMap[String, String](16, 0.75f, c).asScala
    }, "Fastutil" -> { c: Int =>
      new ConcurrentSegmentedMap[String, String](16, 0.75f, c, newFastutilMap)
    }, "Eclipse" -> { c: Int =>
      new ConcurrentSegmentedMap[String, String](16, 0.75f, c, newEclipseMap)
    }, "Scala" -> { c: Int =>
      new ConcurrentSegmentedMap[String, String](16, 0.75f, c, newScalaMap)
    })
    val numOperations = 1000000
    val minNumIters = 10

    for (numItems <- allNumItems) {

      for (numThreads <- allNumThreads) {

        val concurrency = numThreads * 2
        var benchmark = new SparkBenchmark(
          s"Map populate for $numItems items, concurrency = $concurrency",
          numItems,
          minNumIters)

        val maps = new Array[mutable.Map[String, String]](concurrentMapTypes.length)
        concurrentMapTypes.indices.foreach { i =>
          val (name, createMap) = concurrentMapTypes(i)
          benchmark.addCase(name) { _ =>
            maps(i) = createMap(concurrency)
            populateMap(maps(i), numItems)
          }
        }
        benchmark.run()

        benchmark = new SparkBenchmark(
          s"Map reads for $numItems items, threads = $numThreads",
          numOperations * numThreads,
          minNumIters)
        concurrentMapTypes.indices.foreach { i =>
          benchmark.addCase(concurrentMapTypes(i)._1)(_ =>
            runThreaded(numThreads, () => runGets(maps(i), numOperations, numItems), "gets"))
        }
        benchmark.run()

        benchmark = new SparkBenchmark(
          s"Map puts for $numItems items, threads = $numThreads",
          numOperations * numThreads,
          minNumIters)
        concurrentMapTypes.indices.foreach { i =>
          benchmark.addCase(concurrentMapTypes(i)._1)(_ =>
            runThreaded(numThreads, () => runPuts(maps(i), numOperations, numItems), "puts"))
        }
        benchmark.run()

        for (putRatio <- putRatios) {
          benchmark = new SparkBenchmark(
            s"Map puts (ratio=$putRatio) and gets for $numItems items, threads = $numThreads",
            numOperations * numThreads,
            minNumIters)
          concurrentMapTypes.indices.foreach { i =>
            benchmark.addCase(concurrentMapTypes(i)._1) { _ =>
              runThreaded(
                numThreads,
                () => runMixed(maps(i), numOperations, numItems, putRatio),
                s"mixed with putRatio=$putRatio")
            }
          }
          benchmark.run()
        }
      }
    }
  }

  benchmark("iteration comparison") {
    val numItems = 20
    val numSetItems = 5000000
    val step = numSetItems / numItems
    val minNumIters = 100

    val data = new ConcurrentSkipListSet[StoredObject]((o1: StoredObject, o2: StoredObject) => {
      if (o1 ne o2) {
        val cmp = java.lang.Double.compare(o1.weightage, o2.weightage)
        if (cmp != 0) cmp
        else {
          val c = o1.key.asInstanceOf[Comparable[AnyRef]].compareTo(o2.key.asInstanceOf[AnyRef])
          // order compressed having same key to be lower priority than decompressed
          if (c != 0) c
          else java.lang.Long.compare(System.identityHashCode(o1), System.identityHashCode(o2))
        }
      } else 0
    })
    val hashSet = new java.util.HashSet[StoredObject]
    val openHashSet = new ObjectOpenHashSet[StoredObject]
    val linkedHashSet = new java.util.LinkedHashSet[StoredObject]
    val arrayList = new java.util.ArrayList[StoredObject]()
    val linkedList = new java.util.LinkedList[StoredObject]()

    def populateData(collection: java.util.Collection[StoredObject], step: Int): Unit = {
      for (i <- 0 until numSetItems by step) {
        val key = s"someUniqueKeyForTheSet_$i"
        val value = s"someValueForStoredObject_$i".getBytes
        val weightage = 17.8 * (i % 20)
        val weightageWithoutBoost = weightage - 23.1
        val generation = i % 10
        if ((i & 0x1) == 0) {
          collection.add(
            new StoredObject(key, value, weightage, weightageWithoutBoost, generation))
        } else {
          val compressionSavings = weightage / 4.4
          collection.add(
            new CompressedStoredObject(
              key,
              value,
              weightage,
              weightageWithoutBoost,
              generation,
              compressionSavings))
        }
      }
    }
    populateData(data, step = 1)
    populateData(hashSet, step)
    populateData(openHashSet, step)
    populateData(linkedHashSet, step)
    populateData(arrayList, step)
    populateData(linkedList, step)

    def iterateSet(checkCollection: java.util.Set[StoredObject]): Int = {
      val timeWeightage = 204.8
      var result = 0
      val iter = data.iterator()
      while (iter.hasNext) {
        val candidate = iter.next()
        if (!checkCollection.contains(candidate)) {
          val checkIter = checkCollection.iterator()
          while (checkIter.hasNext) {
            val check = checkIter.next()
            if (check.generation > candidate.generation &&
                check.weightage < candidate.weightage + timeWeightage) {
              result += 1
            }
          }
        }
      }
      result
    }
    def iterateData(checkCollection: java.util.Collection[StoredObject]): Int = {
      val timeWeightage = 204.8
      var result = 0
      val iter = data.iterator()
      while (iter.hasNext) {
        val candidate = iter.next()
        var tempResult = 0
        val checkIter = checkCollection.iterator()
        while (tempResult >= 0 && checkIter.hasNext) {
          val check = checkIter.next()
          if (check.equals(candidate)) tempResult = -1
          else if (check.generation > candidate.generation &&
                   check.weightage < candidate.weightage + timeWeightage) {
            tempResult += 1
          }
        }
        if (tempResult > 0) result += tempResult
      }
      result
    }

    val expectedResult = iterateData(hashSet)
    val benchmark = new SparkBenchmark("Compare iteration", numItems * numSetItems, minNumIters)

    benchmark.addCase("Java HashSet") { _ =>
      assert(iterateSet(hashSet) == expectedResult)
    }
    benchmark.addCase("Fastutil OpenHashSet") { _ =>
      assert(iterateSet(openHashSet) == expectedResult)
    }
    benchmark.addCase("Java LinkedHashSet") { _ =>
      assert(iterateSet(linkedHashSet) == expectedResult)
    }
    benchmark.addCase("Java ArrayList") { _ =>
      assert(iterateData(arrayList) == expectedResult)
    }
    benchmark.addCase("Java LinkedList") { _ =>
      assert(iterateData(linkedList) == expectedResult)
    }
    benchmark.run()
  }
}

sealed class StoredObject(
    val key: Comparable[_ <: AnyRef],
    val value: AnyRef,
    val weightage: Double,
    val weightageWithoutBoost: Double,
    val generation: Int)

final class CompressedStoredObject(
    key: Comparable[_ <: AnyRef],
    value: AnyRef,
    weightage: Double,
    weightageWithoutBoost: Double,
    generation: Int,
    val compressionSavings: Double)
    extends StoredObject(key, value, weightage, weightageWithoutBoost, generation)
