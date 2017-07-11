cmake_minimum_required(VERSION 2.8)

# -------------------
# CMake configuration
# -------------------

if(REACTOR_BUILD)
	#list(APPEND CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/protobuf $ENV{RS_BUILDKIT}/boost $ENV{RS_BUILDKIT}/python2)
	list(APPEND CMAKE_PREFIX_PATH $ENV{RS_BUILDKIT}/boost $ENV{RS_BUILDKIT}/poco $ENV{RS_BUILDKIT}/tbb 
                       ./reactor/libs $ENV{RS_BUILDKIT}/perftools $ENV{RS_BUILDKIT}/libunwind)

    

	set(buildkit "$ENV{RS_BUILDKIT}")
	set(CMAKE_CXX_COMPILER $ENV{RS_BUILDKIT}/gcc/bin/g++)
	set(CMAKE_C_COMPILER $ENV{RS_BUILDKIT}/gcc/bin/gcc)
	set(BOOST_ROOT $ENV{RS_BUILDKIT}/boost)
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".so")

# ---------------------
# Project configuration
# ---------------------
project(Tick_Tool)

set(TT_MAJOR 0)
set(TT_MINOR 0)
set(TT_PATCHLEVEL 1)
string(TIMESTAMP COMPILATION_DATE)

# --------
# Compiler
# --------
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++14" COMPILER_SUPPORTS_CXX14)
if(COMPILER_SUPPORTS_CXX14)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
elseif(COMPILER_SUPPORTS_CXX11)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

add_definitions( -fdiagnostics-show-location=once )

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++" )
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++")


# ---------------------------
# Find libraries and packages
# ---------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(Boost_NO_BOOST_CMAKE ON)
set(Boost_USE_MULTITHREADED ON)

if(REACTOR_BUILD)
	set(Boost_USE_STATIC_LIBS ON)
endif()
find_package(Boost REQUIRED COMPONENTS date_time log filesystem system program_options chrono regex serialization timer iostreams python thread unit_test_framework)
#
# Find Poco
#
find_path(Poco_INCLUDE_DIR NAMES Poco/Version.h)
find_library(PocoFoundation_LIBRARY NAMES libPocoFoundation.a PocoFoundationd)
find_library(PocoNet_LIBRARY NAMES libPocoNet.a PocoNetd)
find_library(PocoXML_LIBRARY NAMES libPocoXML.a PocoXMLd)
find_library(PocoUtil_LIBRARY NAMES libPocoUtil.a PocoUtild)
get_filename_component(Poco_LIBRARY_PATH ${PocoFoundation_LIBRARY} PATH)
MESSAGE( STATUS "PocoFoundation_LIBRARY:         " ${PocoFoundation_LIBRARY})
set(Poco_LIBRARY_PATH ${Poco_LIBRARY_PATH} CACHE PATH "Poco library path")
#if(NOT BUILD_SHARED_LIBS)
#    set(RS_LIBRARY_TYPE "SHARED")
#else()
    set(RS_LIBRARY_TYPE "STATIC")
#endif()
add_library(PocoFoundation ${RS_LIBRARY_TYPE} IMPORTED)
set_target_properties(PocoFoundation PROPERTIES
    IMPORTED_LOCATION "${PocoFoundation_LIBRARY}"
    IMPORTED_LINK_INTERFACE_LIBRARIES "pthread;dl")
add_library(PocoUtil ${RS_LIBRARY_TYPE} IMPORTED)
set_target_properties(PocoUtil PROPERTIES
    IMPORTED_LOCATION "${PocoUtil_LIBRARY}"
    IMPORTED_LINK_INTERFACE_LIBRARIES "${PocoXML_LIBRARY}")

find_library(LCC_MSG_LIBRARY NAMES liblcc_msgs_small.a)
#include(RInside)

#
# Find TBB
#
find_path(TBB_INCLUDE_DIR NAMES tbb/tbb.h)
find_library(TBB_LIBRARY NAMES tbb tbb_debug)
get_filename_component(TBB_LIBRARY_PATH ${TBB_LIBRARY} PATH)
set(TBB_LIBRARY_PATH ${TBB_LIBRARY_PATH} CACHE PATH "TBB library path")
add_library(tbb STATIC IMPORTED)
set_target_properties(tbb PROPERTIES
    IMPORTED_LOCATION "${TBB_LIBRARY}"
    IMPORTED_LINK_INTERFACE_LIBRARIES "-L${TBB_LIBRARY_PATH};pthread;dl;rt")


#find gperf
find_library(GPERF_LIB NAMES profiler)
find_path(GPERF_INCLUDE_DIR NAMES gperftools/profiler.h)

#find unwind
find_library(UNWIND_LIB NAMES unwind)

# -------
# Options
# -------
option(COLOUR_COMPILER "Colorize compiler output" ON)

if(COLOUR_COMPILER)
    add_definitions( -fdiagnostics-color=always )
endif()

# ------------------------------------------------------------------------------------------------------------------
# Boost circular buffer has some debug support which can cause issues if you happen to mix release & debug libraries
# so we switch it off in all cases
# ------------------------------------------------------------------------------------------------------------------
add_definitions( -DBOOST_CB_DISABLE_DEBUG=1 )

# -----------------------
# Set or unset debug mode
# -----------------------
string(TOLOWER "${CMAKE_BUILD_TYPE}" compile_type)
if(compile_type MATCHES "debug")
	add_compile_options(-DDEBUG -g -fPIC)
	add_compile_options(-mfpmath=sse -ffast-math)
else()
	add_compile_options(-DNDEBUG -fPIC)
	add_compile_options(-mfpmath=sse -ffast-math)
endif()

# ----------------------------
# Include and link directories
# ----------------------------
include_directories(${PROJECT_SOURCE_DIR})
include_directories(include)
include_directories(3rd-party)
include_directories(products/include)
include_directories(reactor/include)
include_directories(reactor/include/lcc_msg)
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${Poco_INCLUDE_DIR})
include_directories(${GPERF_INCLUDE_DIR})
include_directories(${TBB_INCLUDE_DIR})
#foreach(rdir ${RINSIDE_INCLUDE_DIR})
#	include_directories(${rdir})
#endforeach()

link_directories(${CMAKE_CURRENT_SOURCE_DIR})
#foreach(rdir ${RINSIDE_LIB_DIR})
#	link_directories(${rdir})
#endforeach()

if(REACTOR_BUILD)
	include_directories(${buildkit})
	link_directories(${buildkit})
endif()

if(Boost_FOUND)
	include_directories(${Boost_INCLUDE_DIRS})
	link_directories(${Boost_LIBRARY_DIRS})
endif()

# -----------------------------
# Define libraries and binaries
# -----------------------------
file(GLOB ticktool_HDR "include/*.h")
file(GLOB ticktool_SRC "src/*.cpp" "products/src/*.cpp")

set(qtg2influx_MAIN_SRC "main/main.cpp")
set(testtool_dir_src "main/testtool.cpp")
add_library(ticktool_obj OBJECT ${ticktool_SRC})
set_property(TARGET ${ticktool_obj} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(ticktool SHARED $<TARGET_OBJECTS:ticktool_obj>)
add_library(ticktoolstatic STATIC $<TARGET_OBJECTS:ticktool_obj>)
set_target_properties(ticktoolstatic PROPERTIES OUTPUT_NAME ticktool)

add_executable(qtg2influx ${qtg2influx_MAIN_SRC})
add_executable(testtool ${testtool_dir_src})


# ------------
# Dependencies
# ------------
target_link_libraries(ticktool asm  pthread)
set(ticklib -Wl,--whole-archive ticktool -Wl,--no-whole-archive)

target_link_libraries(qtg2influx -lrt ${GPERF_LIB} ${UNWIND_LIB} ticktoolstatic ${LCC_MSG_LIBRARY}  ${Boost_LIBRARIES} ${TBB_LIBRARY} ${PocoNet_LIBRARY} PocoUtil PocoFoundation pthread)

target_link_libraries(testtool -lrt ${GPERF_LIB} ${UNWIND_LIB} ticktoolstatic ${LCC_MSG_LIBRARY} ${Boost_LIBRARIES} ${TBB_LIBRARY} ${PocoNet_LIBRARY} PocoUtil PocoFoundation  pthread)

