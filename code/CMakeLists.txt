cmake_minimum_required(VERSION 3.8)
project(MyFilesystem)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        myfs.c
        myfs.h
        unqlite.h)

include_directories(.)

add_library(unqlite unqlite.h unqlite.c)

add_definitions(-D_FILE_OFFSET_BITS=64)

add_executable(myfs ${SOURCE_FILES})
target_link_libraries(myfs unqlite uuid fuse)