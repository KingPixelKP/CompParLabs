include(ExternalProject)
include(FetchContent)

set (THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/third_party)

#############################
## LibWB
#############################

include_directories(${THIRD_PARTY_DIR}/libwb)
include(${THIRD_PARTY_DIR}/libwb/sources.cmake)

add_library(${WBLIB} ${LIBWB_SOURCE_FILES})
target_link_libraries(${WBLIB} ${LINK_LIBRARIES})
target_link_libraries(${WBLIB} ${CUDA_cudart_static_LIBRARY})
target_link_libraries(${WBLIB} ${CUDA_LIBRARIES})

set (WBLIB ${WBLIB} PARENT_SCOPE)

#############################
## Google Test
#############################

set (GTEST_HOME ${THIRD_PARTY_DIR}/googletest)

add_subdirectory(googletest)

set (GTEST_HOME ${GTEST_HOME} PARENT_SCOPE)
set (GTEST_BOTH_LIBRARIES gtest gtest_main PARENT_SCOPE)
set (THIRD_PARTY_DIR ${THIRD_PARTY_DIR} PARENT_SCOPE)
