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

package com.github.spark.lightspeed.platform;

import com.github.spark.lightspeed.platform.internal.PosixOperatingSystem;
import com.github.spark.lightspeed.platform.internal.WindowsOperatingSystem;
import com.sun.jna.LastErrorException;
import com.sun.jna.Platform;

/**
 * Some common native OS-level methods having no equivalent in Java/Scala using JNA calls.
 */
public interface OperatingSystem {

  /**
   * Get the process ID of the current process.
   */
  int getProcessId();

  /**
   * Set the current working directory of this process. The provided directory can
   * be relative or absolute but should already exist.
   */
  void setCurrentWorkingDirectory(String dir) throws LastErrorException;

  /**
   * Get an instance of {@link OperatingSystem} implementation for the current platform.
   * Currently only POSIX compatible platforms and Windows is supported.
   */
  static OperatingSystem getInstance() {
    return Platform.isWindows() ? WindowsOperatingSystem.getInstance()
        : PosixOperatingSystem.getInstance();
  }
}
