cmake_minimum_required (VERSION 2.6)

ENABLE_LANGUAGE(CXX)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)

set(GCC_COVERAGE_COMPILE_FLAGS "-std=c++14 -g -O0")
#set(GCC_COVERAGE_COMPILE_FLAGS "-std=c++14 -O3")
#set(GCC_COVERAGE_COMPILE_FLAGS "-std=c++14 -O3")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )

find_path(STDIO_INCLUDE_PATH stdio.h)
include_directories("${STDIO_INCLUDE_PATH}/dummy/../")
find_path(STRING_INCLUDE_PATH string /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/include/c++/v1)
include_directories("${STRING_INCLUDE_PATH}/dummy/../")

######################## Boost ######################

SET(BOOST_ROOT "/Users/elgood/libraries/boost_1_66_0")
find_package(Boost REQUIRED system thread program_options serialization
              unit_test_framework)
include_directories(${Boost_INCLUDE_DIRS})

##################### Protobuf ######################

find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})

###################### ZMQ ##########################

find_path(ZMQ_INCLUDE_DIR zmq.h)
find_library(ZMQ_LIBRARY NAMES zmq)
set (ZMQ_LIBRARIES ${ZMQ_LIBRARY})
set (ZMQ_INCLUDE_DIRS ${ZMQ_INCLUDE_DIR})

include ( FindPackageHandleStandardArgs )
find_package_handle_standard_args(ZMQ DEFAULT_MSG ZMQ_LIBRARY ZMQ_INCLUDE_DIR)

include_directories("${ZMQ_INCLUDE_DIRS}")

###################### Armadillo ##############################

#find_package(Armadillo REQUIRED)
#include_directories(${ARMADILLO_INCLUDE_DIRS})

####################### MLPACK #########################
#find_library(MLPACK_LIBRARY NAMES mlpack)
#set (MLPACK_LIBRARIES ${MLPACK_LIBRARY})

####################### Include directories #################
include_directories(SamSrc)

################ ReadSocket ################

#file(GLOB READ_SOCKET_SOURCES "ExecutableSrc/TestNCSpeed.cpp")

#add_executable(ReadSocket ${READ_SOCKET_SOURCES} )
#set_target_properties(ReadSocket PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

#target_link_libraries(ReadSocket ${BOOST_PROGRAM_OPTIONS_LIBRARY})
#target_link_libraries(ReadSocket SamLib)

############# The SAM Library ################

#file (GLOB SAM_SOURCES "SamSrc/*.cpp")

#add_library(SamLib ${SAM_SOURCES})
add_library(SamLib INTERFACE)
target_include_directories(SamLib INTERFACE SamSrc/)
#set_target_properties(SamLib PROPERTIES LIBRARY_OUTPUT_DIRECTORY "lib")

#target_link_libraries(SamLib ${ZMQ_LIBRARIES})
#target_link_libraries(SamLib boost_program_options)
#target_link_libraries(SamLib boost_thread)
#target_link_libraries(SamLib boost_system)

############# Protobuf ######################
file (GLOB PROTO_SOURCES "SamSrc/protobuf/*.cc")
add_library(ProtoLib ${PROTO_SOURCES})
set_target_properties(ProtoLib PROPERTIES LIBRARY_OUTPUT_DIRECTORY "lib") 
include_directories(SamSrc/protobuf)

################# Executables ###############################
file (GLOB EXE_SRCS ExecutableSrc/*.cpp)

foreach(exeSrc ${EXE_SRCS})
  get_filename_component(exeName ${exeSrc} NAME_WE)

  add_executable(${exeName} ${exeSrc})

  target_link_libraries(${exeName} SamLib)
  target_link_libraries(${exeName} pthread)
  target_link_libraries(${exeName} ${ZMQ_LIBRARIES})
  target_link_libraries(${exeName} ${Boost_LIBRARIES})
  target_link_libraries(${exeName} ${PROTOBUF_LIBRARIES})
  target_link_libraries(${exeName} ProtoLib)

  set_target_properties(${exeName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")
endforeach(exeSrc)


############################## Testing ######################
enable_testing()

file (GLOB TEST_SRCS TestSrc/*.cpp)

foreach(testSrc ${TEST_SRCS})
  get_filename_component(testName ${testSrc} NAME_WE)

  add_definitions(-DBOOST_TEST_DYN_LINK)
  add_executable(${testName} ${testSrc})

  target_link_libraries(${testName} SamLib)
  target_link_libraries(${testName} ${Boost_LIBRARIES})
  #target_link_libraries(${testName} boost_unit_test_framework)
  #target_link_libraries(${testName} boost_system)
  target_link_libraries(${testName} pthread)
  #target_link_libraries(${testName} boost_thread)
  target_link_libraries(${testName} ${ZMQ_LIBRARIES})
  target_link_libraries(${testName} ${PROTOBUF_LIBRARIES})
  target_link_libraries(${testName} ProtoLib)

  set_target_properties(${testName} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

  add_test(NAME ${testName} 
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests
           COMMAND ${CMAKE_BINARY_DIR}/tests/${testName})
endforeach(testSrc)


