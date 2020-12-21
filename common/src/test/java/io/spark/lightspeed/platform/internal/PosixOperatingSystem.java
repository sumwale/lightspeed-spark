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

package io.spark.lightspeed.platform.internal;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import io.spark.lightspeed.platform.OperatingSystem;

/**
 * Implementation of {@link OperatingSystem} for POSIX compatible platforms.
 */
public class PosixOperatingSystem implements OperatingSystem {

  static {
    Native.register(Platform.C_LIBRARY_NAME);

    instance = new PosixOperatingSystem();
  }

  private static final PosixOperatingSystem instance;

  public static native int chdir(String path) throws LastErrorException;

  public static native int getpid();

  public static OperatingSystem getInstance() {
    return instance;
  }

  @Override
  public int getProcessId() {
    return getpid();
  }

  @Override
  public void setCurrentWorkingDirectory(String dir) throws LastErrorException {
    System.setProperty("user.dir", dir);
    chdir(dir);
  }
}
