#include <wb.h>


#define BLUR_SIZE 5 // The halo of the Stencil computation

#define TILE_WIDTH 16

/**
 * Blur filter
 *
 * @param out The buffer to receive the blurred image to compute
 * @param in The image to blur
 * @param width Width of in
 * @param height Height of in
 */
__global__ void blur_kernel(float *out, float *in, int width, int height) {
    // TODO
}


wbImage_t image_blur(wbImage_t inputImage) {
    const int imageWidth = wbImage_getWidth(inputImage);
    const int imageHeight = wbImage_getHeight(inputImage);

    const int imageChannels = 1; // Assuming grayscale images
    float *hostInputImageData = wbImage_getData(inputImage);

    // Since the image is monochromatic, it only contains one channel
    wbImage_t outputImage = wbImage_new(imageWidth, imageHeight, imageChannels);
    float *hostOutputImageData = wbImage_getData(outputImage);

         // TODO

    // declare buffers for the images in the device

    // Allocate memory for the buffers in the device.
    // The size of each buffer is imageWidth * imageHeight * sizeof(float))

    // Copy the source image to the device

    // Define a 2-dimensional thread grid of tiles TILE_WIDTH x TILE_WIDTH

    // Execute the kernel

    // Copy the result to the main memory

    // Free the buffers on the device

    return outputImage;
}

int main(int argc, char *argv[]) {

    if (argc != 2) {
        printf ("usage %s image_file.ppm\n.", argv[0]);
        return 1;
    }
    const char *inputImageFile = argv[1];

    wbImage_t inputImage = wbImport(inputImageFile);
    wbImage_t outputImage = image_blur(inputImage);
    wbExport("output.ppm", outputImage);

    wbImage_delete(outputImage);
    wbImage_delete(inputImage);

    return 0;
}