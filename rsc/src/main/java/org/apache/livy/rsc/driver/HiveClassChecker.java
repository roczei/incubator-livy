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

package org.apache.livy.rsc.driver;

final class HiveClassChecker {

  private static final String HIVE_SESSION_STATE_BUILDER =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder";
  private static final String HIVE_CONF = "org.apache.hadoop.hive.conf.HiveConf";

  private HiveClassChecker() {
  }

  static boolean hiveClassesArePresent() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = HiveClassChecker.class.getClassLoader();
    }
    try {
      Class.forName(HIVE_SESSION_STATE_BUILDER, false, loader);
      Class.forName(HIVE_CONF, false, loader);
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }
}
