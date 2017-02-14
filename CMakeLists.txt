cmake_minimum_required (VERSION 2.6)

ENABLE_LANGUAGE(CXX)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)

set(GCC_COVERAGE_COMPILE_FLAGS "-std=c++11 -O3")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )

find_path(STDIO_INCLUDE_PATH stdio.h)
include_directories("${STDIO_INCLUDE_PATH}/dummy/../")
find_path(STRING_INCLUDE_PATH string /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/include/c++/v1)
include_directories("${STRING_INCLUDE_PATH}/dummy/../")

######################## Boost ######################

SET(BOOST_ROOT "/Users/elgood/libraries/boost_1_62_0")
find_package(Boost REQUIRED system thread program_options)
include_directories(${Boost_INCLUDE_DIRS})

###################### ZMQ ##########################

find_path(ZMQ_INCLUDE_DIR zmq.h)
find_library(ZMQ_LIBRARY NAMES zmq)
set (ZMQ_LIBRARIES ${ZMQ_LIBRARY})
set (ZMQ_INCLUDE_DIRS ${ZMQ_INCLUDE_DIR})

include ( FindPackageHandleStandardArgs )
find_package_handle_standard_args(ZMQ DEFAULT_MSG ZMQ_LIBRARY ZMQ_INCLUDE_DIR)

include_directories("${ZMQ_INCLUDE_DIRS}")

####################### Include directories #################
include_directories(SamSrc)

################ ReadSocket ################

file(GLOB READ_SOCKET_SOURCES "ExecutableSrc/TestNCSpeed.cpp")

add_executable(ReadSocket ${READ_SOCKET_SOURCES} )
set_target_properties(ReadSocket PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(ReadSocket ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(ReadSocket SamLib)

############# The SAM Library ################

file (GLOB SAM_SOURCES "SamSrc/*.cpp")

add_library(SamLib ${SAM_SOURCES})
set_target_properties(SamLib PROPERTIES LIBRARY_OUTPUT_DIRECTORY "lib")

target_link_libraries(SamLib ${ZMQ_LIBRARIES})
target_link_libraries(SamLib boost_program_options)
target_link_libraries(SamLib boost_thread)
target_link_libraries(SamLib boost_system)

############# PushPull ###################

file (GLOB PUSH_PULL_SOURCES "ExecutableSrc/TestPushPull.cpp")

add_executable(PushPull ${PUSH_PULL_SOURCES} )
set_target_properties(PushPull PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(PushPull ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(PushPull ${ZMQ_LIBRARIES})
target_link_libraries(PushPull SamLib)
target_link_libraries(PushPull pthread)

############### TopK #####################

file (GLOB TOPK_SOURCES "ExecutableSrc/TestTopK.cpp")

add_executable(TopK ${TOPK_SOURCES})
set_target_properties(TopK PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(TopK ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(TopK ${ZMQ_LIBRARIES})
target_link_libraries(TopK SamLib)
target_link_libraries(TopK pthread)

############### SimpleSum #####################

file (GLOB SIMPLE_SUM_SOURCES "ExecutableSrc/TestSimpleSum.cpp")

add_executable(SimpleSum ${SIMPLE_SUM_SOURCES})
set_target_properties(SimpleSum PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(SimpleSum ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(SimpleSum ${ZMQ_LIBRARIES})
target_link_libraries(SimpleSum SamLib)
target_link_libraries(SimpleSum pthread)

############### ExponentialHistogramSum #####################

file (GLOB EH_SUM_SOURCES "ExecutableSrc/TestExponentialHistogramSumOperator.cpp")

add_executable(ExponentialHistogramSum ${EH_SUM_SOURCES})
set_target_properties(ExponentialHistogramSum PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(ExponentialHistogramSum ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(ExponentialHistogramSum ${ZMQ_LIBRARIES})
target_link_libraries(ExponentialHistogramSum SamLib)
target_link_libraries(ExponentialHistogramSum pthread)

############### ExponentialHistogramVariance #####################

file (GLOB EH_VARIANCE_SOURCES "ExecutableSrc/TestExponentialHistogramVarianceOperator.cpp")

add_executable(ExponentialHistogramVariance ${EH_VARIANCE_SOURCES})
set_target_properties(ExponentialHistogramVariance PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(ExponentialHistogramVariance ${BOOST_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(ExponentialHistogramVariance ${ZMQ_LIBRARIES})
target_link_libraries(ExponentialHistogramVariance SamLib)
target_link_libraries(ExponentialHistogramVariance pthread)


############ Test Active Window #######################

file (GLOB TEST_ACTIVE_WINDOW_SOURCES "TestSrc/TestActiveWindow.cpp")

add_definitions(-DBOOST_TEST_DYN_LINK)

add_executable(TestActiveWindow ${TEST_ACTIVE_WINDOW_SOURCES})
set_target_properties(TestActiveWindow PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

target_link_libraries(TestActiveWindow boost_unit_test_framework)

############ Test Active Window #######################

file (GLOB TEST_DORMANT_WINDOW_SOURCES "TestSrc/TestDormantWindow.cpp")

add_definitions(-DBOOST_TEST_DYN_LINK)

add_executable(TestDormantWindow ${TEST_DORMANT_WINDOW_SOURCES})
set_target_properties(TestDormantWindow PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

target_link_libraries(TestDormantWindow boost_unit_test_framework)

############ Test Sliding Window #######################

file (GLOB TEST_SLIDING_WINDOW_SOURCES "TestSrc/TestSlidingWindow.cpp")

add_definitions(-DBOOST_TEST_DYN_LINK)

add_executable(TestSlidingWindow ${TEST_SLIDING_WINDOW_SOURCES})
set_target_properties(TestSlidingWindow PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

target_link_libraries(TestSlidingWindow boost_unit_test_framework)

############ Test SimpleSum #############################

file (GLOB TEST_SIMPLE_SUM_SOURCES "TestSrc/TestSimpleSum.cpp")

add_executable(TestSimpleSum ${TEST_SIMPLE_SUM_SOURCES})
set_target_properties(TestSimpleSum PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

target_link_libraries(TestSimpleSum boost_unit_test_framework)
target_link_libraries(TestSimpleSum SamLib)

############ Test ExponentialHistogram #################

file (GLOB TEST_EH_SOURCES "TestSrc/TestExponentialHistogram.cpp")

add_executable(TestExponentialHistogram ${TEST_EH_SOURCES})
set_target_properties(TestExponentialHistogram PROPERTIES RUNTIME_OUTPUT_DIRECTORY "tests")

target_link_libraries(TestExponentialHistogram boost_unit_test_framework)
target_link_libraries(TestExponentialHistogram SamLib)
