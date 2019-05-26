cmake_minimum_required(VERSION 3.0)

find_package(Java COMPONENTS Runtime)
if(JAVA_FOUND)
  include(UseJava)
else()
  message(WARNING "Couldn't find Java")
endif()

find_jar(SCALA_JAR "scala-library")
message("scala-library.jar is at ${SCALA_JAR}")
