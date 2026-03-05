cmake_minimum_required(VERSION 3.15)
project(nitrocoro-redis)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(NITROCORO_BUILD_DIR "" CACHE PATH "NitroCoro build directory (preferred)")
set(NITROCORO_SOURCE_DIR "" CACHE PATH "NitroCoro source directory (fallback)")

if (NITROCORO_BUILD_DIR)
    if (NOT NITROCORO_SOURCE_DIR)
        if (EXISTS "${NITROCORO_BUILD_DIR}/CMakeCache.txt")
            file(STRINGS "${NITROCORO_BUILD_DIR}/CMakeCache.txt" _source_dir_line
                REGEX "^CMAKE_HOME_DIRECTORY:INTERNAL=")
            if (_source_dir_line)
                string(REGEX REPLACE "^CMAKE_HOME_DIRECTORY:INTERNAL=" "" NITROCORO_SOURCE_DIR "${_source_dir_line}")
                message(STATUS "Deduced NITROCORO_SOURCE_DIR from CMakeCache.txt: ${NITROCORO_SOURCE_DIR}")
            endif ()
        endif ()

        if (NOT NITROCORO_SOURCE_DIR)
            get_filename_component(NITROCORO_SOURCE_DIR "${NITROCORO_BUILD_DIR}/.." ABSOLUTE)
            message(STATUS "Deduced NITROCORO_SOURCE_DIR from parent directory: ${NITROCORO_SOURCE_DIR}")
        endif ()
    endif ()

    find_library(NITROCORO_LIB nitrocoro PATHS ${NITROCORO_BUILD_DIR} NO_DEFAULT_PATH REQUIRED)
    add_library(nitrocoro STATIC IMPORTED)
    set_target_properties(nitrocoro PROPERTIES
        IMPORTED_LOCATION ${NITROCORO_LIB}
        INTERFACE_INCLUDE_DIRECTORIES "${NITROCORO_SOURCE_DIR}/include"
    )
elseif (NITROCORO_SOURCE_DIR)
    add_subdirectory(${NITROCORO_SOURCE_DIR} ${CMAKE_BINARY_DIR}/nitrocoro)
else ()
    find_package(nitrocoro QUIET)
    if (NOT nitrocoro_FOUND)
        message(FATAL_ERROR "NitroCoro not found. Please set NITROCORO_BUILD_DIR, NITROCORO_SOURCE_DIR, or install nitrocoro")
    endif ()
endif ()

add_library(nitrocoro-redis STATIC
    src/RedisConnectionImpl.cc
    src/RedisResult.cc
    src/RedisPool.cc
    src/PoolState.cc
    src/PooledConnection.cc
)

target_include_directories(nitrocoro-redis PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(nitrocoro-redis PUBLIC nitrocoro hiredis)

# Installation
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS nitrocoro-redis
    EXPORT nitrocoro-redisTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/nitrocoro
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
)

install(EXPORT nitrocoro-redisTargets
    FILE nitrocoro-redisTargets.cmake
    NAMESPACE nitrocoro::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nitrocoro-redis
)

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/nitrocoro-redisConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/nitrocoro-redisConfig.cmake
    @ONLY
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/nitrocoro-redisConfig.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nitrocoro-redis
)

option(BUILD_TESTING "Build tests" ON)

if (BUILD_TESTING)
    enable_testing()

    add_executable(redis_test tests/redis_test.cc)
    target_link_libraries(redis_test PRIVATE nitrocoro-redis)
    add_test(NAME redis_test COMMAND redis_test)

    add_executable(redis_pool_test tests/redis_pool_test.cc)
    target_link_libraries(redis_pool_test PRIVATE nitrocoro-redis)
    add_test(NAME redis_pool_test COMMAND redis_pool_test)
endif ()
