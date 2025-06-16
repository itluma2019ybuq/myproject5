name := "ProductionOptimization"

version := "1.0"

scalaVersion := "2.12.17"

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.14" % Test
)

// 解决依赖冲突
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.4"
)

// 编译选项
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

// 运行时选项
fork := true
javaOptions ++= Seq(
  "-Xmx4g",
  "-XX:+UseG1GC"
) 