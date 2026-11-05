
set(WBLIB "wb")

include_directories(.)

if (UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-dollar-in-identifier-extension")
endif ()

file(GLOB THESE_CPP_FILES
    ${CMAKE_CURRENT_LIST_DIR}/*.cpp
)


file(GLOB THESE_HEADER_FILES
    ${CMAKE_CURRENT_LIST_DIR}/*.h
)

list(APPEND LIBWB_HEADER_FILES
    ${THESE_HEADER_FILES}
)

list(APPEND LIBWB_SOURCE_FILES
    ${THESE_CPP_FILES}
        ${CMAKE_CURRENT_LIST_DIR}/vendor/json11.cpp
)

list(APPEND LIBWB_TEST_FILES
    ${THESE_TEST_FILES}
)
