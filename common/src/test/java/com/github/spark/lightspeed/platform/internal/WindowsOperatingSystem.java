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

package com.github.spark.lightspeed.platform.internal;

import com.github.spark.lightspeed.platform.OperatingSystem;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.win32.W32APIOptions;

/**
 * Implementation of {@link OperatingSystem} for Windows platform.
 */
public class WindowsOperatingSystem implements OperatingSystem {

  static {
    NativeLibrary kernel32Lib = NativeLibrary.getInstance("kernel32",
        W32APIOptions.DEFAULT_OPTIONS);
    Native.register(kernel32Lib);

    instance = new WindowsOperatingSystem();
  }

  private static final WindowsOperatingSystem instance;

  public static native boolean SetCurrentDirectory(String path) throws LastErrorException;

  public static native int GetCurrentProcessId();

  public static OperatingSystem getInstance() {
    return instance;
  }

  @Override
  public int getProcessId() {
    return GetCurrentProcessId();
  }

  @Override
  public void setCurrentWorkingDirectory(String dir) throws LastErrorException {
    System.setProperty("user.dir", dir);
    SetCurrentDirectory(dir);
  }
}
