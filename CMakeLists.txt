cmake_minimum_required (VERSION 2.6)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY lib)

set(GCC_COVERAGE_COMPILE_FLAGS "-std=c++11")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )

add_library(boost_program_options SHARED IMPORTED)
set_target_properties(boost_program_options PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libboost_program_options.dylib")

################ ReadSocket ################
project(ReadSocket)

include_directories("/Users/elgood/libraries/boost_1_62_0")
include_directories(ReadSocketSrc)

file(GLOB READ_SOCKET_SOURCES "ReadSocketSrc/*.cpp")

add_executable(ReadSocket ${READ_SOCKET_SOURCES} )
set_target_properties(ReadSocket PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")


target_link_libraries(ReadSocket boost_program_options)


################ ReadSocketLib #############
project(ReadSocketLib)

file(GLOB READ_SOCKET_LIB_SOURCES "ReadSocketSrc/ReadSocket.cpp")

add_library(ReadSocketLib ${READ_SOCKET_LIB_SOURCES})
set_target_properties(ReadSocketLib PROPERTIES LIBRARY_OUTPUT_DIRECTORY "lib")

target_link_libraries(ReadSocketLib boost_program_options)

############# PushPull ###################
project(PushPull)

file (GLOB PUSH_PULL_SOURCES "PushPullSrc/*.cpp")

add_executable(PushPull ${PUSH_PULL_SOURCES} )
set_target_properties(PushPull PROPERTIES RUNTIME_OUTPUT_DIRECTORY "bin")

target_link_libraries(PushPull boost_program_options)
target_link_libraries(PushPull ReadSocketLib)

add_library(ZMQ SHARED IMPORTED)
set_target_properties(ZMQ PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libzmq.dylib")

target_link_libraries(PushPull ZMQ)

############# Netflow ################
project(NetflowLib)

file (GLOB NETFLOW_SOURCES "NetflowSrc/*.cpp")

add_library(NetflowLib ${NETFLOW_SOURCES})
set_target_properties(NetflowLib PROPERTIES LIBRARY_OUTPUT_DIRECTORY "lib")

target_link_libraries(NetflowLib boost_program_options)
