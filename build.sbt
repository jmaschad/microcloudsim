name := "MicroCloudSim"

version := "0.1"

resolvers += "twitter" at "http://maven.twttr.com"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.1.+"

libraryDependencies += "com.twitter" % "util-eval" % "5.+"

fork := true

javaOptions += "-Xmx1G"