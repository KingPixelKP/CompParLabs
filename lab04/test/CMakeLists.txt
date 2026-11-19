message(STATUS "Configuring tests")


add_executable(vector_add_cuda_test vector_add_cuda_test.cu )
target_link_libraries(vector_add_cuda_test PRIVATE GTest::gtest_main wb container_ops)
gtest_discover_tests(vector_add_cuda_test)

add_executable(image_color_to_grayscale_test image_color_to_grayscale_test.cu ../src/image_color_to_grayscale.cu)
target_link_libraries(image_color_to_grayscale_test PRIVATE GTest::gtest_main wb container_ops)
gtest_discover_tests(image_color_to_grayscale_test)

