cmake_minimum_required(VERSION 3.18)
project(cuda-intro LANGUAGES CXX CUDA)

set(CMAKE_CUDA_FLAGS "" CACHE STRING "")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(CMAKE_CUDA_FLAGS)
    list(REMOVE_ITEM CMAKE_CUDA_FLAGS "--cudart static")
endif()

string(APPEND CMAKE_CUDA_FLAGS "--cudart shared")

set(CMAKE_CXX_STANDARD 17)

set(VECTORIZATION FALSE)

# #############################################################
# Autodetect CUDA architecture(s)
# #############################################################
include(FindCUDA/select_compute_arch)
CUDA_DETECT_INSTALLED_GPUS(INSTALLED_GPU_CCS_1)
string(STRIP "${INSTALLED_GPU_CCS_1}" INSTALLED_GPU_CCS_2)
string(REPLACE " " ";" INSTALLED_GPU_CCS_3 "${INSTALLED_GPU_CCS_2}")
string(REPLACE "." "" CUDA_ARCH_LIST "${INSTALLED_GPU_CCS_3}")
SET(CMAKE_CUDA_ARCHITECTURES ${CUDA_ARCH_LIST})
set_property(GLOBAL PROPERTY CUDA_ARCHITECTURES "${CUDA_ARCH_LIST}")

cuda_select_nvcc_arch_flags(ARCH_FLAGS Auto)
list(APPEND CUDA_NVCC_FLAGS ${ARCH_FLAGS})
message("-- CUDA Architecture(s): " ${CMAKE_CUDA_ARCHITECTURES})

add_subdirectory(third_party)
include_directories(${THIRD_PARTY_DIR}/libwb)

# #############################################################
# Build executables
# #############################################################
if(${VECTORIZATION})
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # using GCC/G++
        set(CMAKE_CXX_FLAGS "-O2 -ftree-vectorize -fopt-info-vec-optimized")
    endif()
endif()

include_directories(include)

add_subdirectory(src)

# #############################################################
# Tests
# #############################################################
include_directories(${GTEST_HOME}/googletest/include)
add_subdirectory(test)
