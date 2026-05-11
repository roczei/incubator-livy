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

package org.apache.livy.repl

import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Paths}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results.Result

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

/**
 * Scala 2.13 / Spark 4 variant of SparkInterpreter.
 *
 * Key API differences from the Scala 2.12 variant:
 *  - JPrintWriter was removed; use java.io.PrintWriter directly.
 *  - SparkILoop constructor takes BufferedReader (not Option[BufferedReader]); pass null
 *    for a no-input (batch/embedded) loop.
 *  - SparkILoop.settings is now a val — pass Settings to createInterpreter(settings) instead.
 *  - createInterpreter() now requires a Settings argument and handles full initialization
 *    internally; there is no separate initializeSynchronous() call.
 *  - sparkILoop.intp is typed as scala.tools.nsc.interpreter.Repl (not IMain); bind,
 *    beQuietDuring and valueOfTerm are all members of Repl.
 *  - ILoop.compilerClasspath / ensureClassLoader no longer exist; the classpath-scanning
 *    loop calls addUrlsToClassPath directly.
 *  - Repl.valueOfTerm() is properly implemented in Scala 2.13, so we use it directly
 *    instead of the lastRequest.lineRep.call("$result") workaround used in 2.12.
 */
class SparkInterpreter(protected override val conf: SparkConf) extends AbstractSparkInterpreter {

  private var sparkILoop: SparkILoop = _

  override def start(): Unit = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    val settings = new Settings()
    settings.processArguments(
      List("-Yrepl-class-based", "-Yrepl-outdir", outputDir.getAbsolutePath), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    // Spark 4 / Scala 2.13: constructor takes BufferedReader (not Option); null = no input.
    // settings is a val — pass it to createInterpreter which also handles initialization.
    sparkILoop = new SparkILoop(null, new PrintWriter(outputStream, true))
    sparkILoop.createInterpreter(settings)

    restoreContextClassLoader {
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName ==
            "org.apache.spark.util.MutableURLClassLoader") {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }
            .filterNot { u =>
              Paths.get(u.toURI).getFileName.toString.contains("org.scala-lang_scala-reflect")
            }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkILoop.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      postStart()
    }
  }

  override def close(): Unit = synchronized {
    super.close()

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override def addJar(jar: String): Unit = {
    sparkILoop.addUrlsToClassPath(new URL(jar))
  }

  override protected def isStarted(): Boolean = sparkILoop != null

  override protected def interpret(code: String): Result = sparkILoop.interpret(code)

  override protected def completeCandidates(code: String, cursor: Int): Array[String] = {
    // Use Repl.presentationCompile (public API in Scala 2.13) to get completion candidates.
    // PresentationCompilationResult.candidates(cursor) returns (newCursor, List[String]).
    sparkILoop.intp.presentationCompile(cursor, code) match {
      case Right(result) =>
        try {
          result.candidates(cursor)._2.toArray
        } finally {
          result.cleanup()
        }
      case _ => Array()
    }
  }

  // IMain.valueOfTerm() is properly implemented in Scala 2.13 (it always returned None
  // in 2.12, hence the lastRequest.lineRep workaround used in the scala-2.12 variant).
  override protected def valueOfTerm(name: String): Option[Any] = {
    sparkILoop.intp.valueOfTerm(name)
  }

  override protected def bind(
      name: String,
      tpe: String,
      value: Object,
      modifier: List[String]): Unit = {
    sparkILoop.intp.beQuietDuring {
      sparkILoop.intp.bind(name, tpe, value, modifier)
    }
  }
}
