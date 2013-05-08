seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

name := "MicroCloudSim"

version := "0.8"

resolvers += "twitter" at "http://maven.twttr.com"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.1.+"

libraryDependencies += "com.twitter" % "util-eval" % "5.+"

libraryDependencies += "joda-time" % "joda-time" % "2.+"

libraryDependencies += "org.joda" % "joda-convert" % "1.+"

fork := true

javaOptions += "-Xmx2G"
