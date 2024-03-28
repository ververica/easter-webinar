/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.ververica"
ThisBuild / scalaVersion := "3.3.3"

lazy val flinkVersion = "1.18.1"

def excludeJars(cp: Classpath) =
  cp filter { f =>
    Set(
      "scala-asm-9.4.0-scala-1.jar",
      "interface-1.3.5.jar",
      "interface-1.0.19.jar",
      "scala-compiler-2.13.10.jar",
      "scala3-compiler_3-3.3.3.jar",
      "jline-terminal-3.19.0.jar",
      "jline-reader-3.19.0.jar",
      "jline-3.22.0.jar",
      "flink-shaded-zookeeper-3-3.7.1-17.0.jar"
    ).contains(
      f.data.getName
    )
  }

lazy val root = (project in file(".")).settings(
  name := "lab-easter-webinar",
  libraryDependencies ++= Seq(
    ("org.flinkextended" %% "flink-scala-api" % s"${flinkVersion}_1.1.4")
      .excludeAll(
        ExclusionRule(organization = "org.apache.flink"),
        ExclusionRule(organization = "org.scalameta"),
        ExclusionRule(organization = "com.google.code.findbugs")
      ),
    "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
    "org.apache.flink" % "flink-json" % flinkVersion % Provided,
    "org.apache.flink" % "flink-runtime-web" % flinkVersion % Provided,
    "org.apache.flink" % "flink-connector-base" % flinkVersion % Provided,
    "org.apache.flink" % "flink-connector-kafka" % "3.1.0-1.18" % Provided,
    "ch.qos.logback" % "logback-classic" % "1.4.14" % Provided,
    "com.lihaoyi" %% "upickle" % "3.2.0",
  ),
  assemblyPackageScala / assembleArtifact := false,
  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value
    excludeJars(cp)
  }
)

// make run command include the provided dependencies
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true
