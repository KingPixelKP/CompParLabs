enable_testing()

set(TEST_TARGET "cuda_tests")

set (TEST_SOURCES
        vector_add_cuda_test.cu
        image_color_to_grayscale_test.cu
        ../src/image_color_to_grayscale.cu
)

add_executable(${TEST_TARGET} ${TEST_SOURCES})
target_link_libraries(${TEST_TARGET} ${GTEST_BOTH_LIBRARIES} wb)

