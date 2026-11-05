add_executable(card_info card_info.cu)
add_executable(vector_add vector_add_cuda.cu)


#############################
## image_color_to_grayscale
#############################
add_executable(image_color_to_grayscale
        image_color_to_grayscale.cu
        image_color_to_grayscale_main.cu)
target_link_libraries(image_color_to_grayscale ${WBLIB})

#############################
## image_blur
#############################
add_executable(image_blur image_blur.cu)
target_link_libraries(image_blur ${WBLIB})