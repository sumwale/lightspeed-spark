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

import java.util.concurrent.{ConcurrentHashMap, CyclicBarrier}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.github.spark.lightspeed.SparkSuite
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
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
}
