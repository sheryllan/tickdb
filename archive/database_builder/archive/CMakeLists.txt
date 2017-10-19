cmake_minimum_required(VERSION 2.8)
set( CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake )
project(tickdatabase)

# Compiler
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
if(COMPILER_SUPPORTS_CXX11)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()
add_definitions( -fdiagnostics-show-location=once )
add_compile_options(-DNDEBUG -g -fPIC -mfpmath=sse -ffast-math -O3)

# Boost
set(Boost_NO_BOOST_CMAKE ON)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS filesystem regex)
if(Boost_FOUND)
	include_directories(${Boost_INCLUDE_DIRS})
endif()

# Target
add_executable(pcapfilter "pcapfilter.cpp")
target_link_libraries(pcapfilter ${Boost_LIBRARIES} ${LIBS})
install(TARGETS pcapfilter DESTINATION bin)
