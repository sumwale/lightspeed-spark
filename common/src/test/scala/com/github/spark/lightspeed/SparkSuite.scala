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

package com.github.spark.lightspeed

import java.nio.file.{Files, Paths}

import com.github.spark.lightspeed.platform.OperatingSystem
import org.scalactic.source
import org.scalatest.Tag

import org.apache.spark.SparkFunSuite

abstract class SparkSuite extends SparkFunSuite {

  assert(SparkSuite.processWorkingDir.nonEmpty)
  logDebug(s"Current working directory = ${SparkSuite.processWorkingDir}")

  protected final val runBenchmarks: Boolean =
    java.lang.Boolean.getBoolean("benchmarks.enable")

  protected def benchmark(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: source.Position): Unit = {
    if (runBenchmarks) test(testName, testTags: _*)(testFun)(pos)
  }
}

object SparkSuite {
  private lazy val processWorkingDir: String = {
    val os = OperatingSystem.getInstance()
    val dirName = "jvm_pid" + os.getProcessId
    val dir = Paths.get(dirName).toAbsolutePath
    val dirPath = dir.toString
    assert(Files.exists(Files.createDirectories(dir)))
    os.setCurrentWorkingDirectory(dirPath)
    dirPath
  }
}
