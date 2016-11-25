name := "temdesconto"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.8.8",
  "com.github.scopt"       %% "scopt" % "3.5.0",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)