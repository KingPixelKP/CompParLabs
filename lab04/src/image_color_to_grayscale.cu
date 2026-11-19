#include <wb.h>

#define TILE_WIDTH 16

/**
 * Kernel that converts a RGB image to gray image
 * @param grayImage The buffer to receive the grayscale image being computed
 * @param rgbImage The RGB image to convert
 * @param width Width of rgbImage
 * @param height Height of rgbImage
 */
__global__ void rgb2gray_cuda(float *grayImage, float *rgbImage, int width,
                              int height) {
  int ii = blockIdx.y * blockDim.y + threadIdx.y;
  int jj = blockIdx.x * blockDim.x + threadIdx.x;

  if (ii < height && jj < width) {
    int rgbIndex = (ii * width + jj) * 3; // Each pixel has 3 channels (R, G, B)
    int grayIndex = ii * width + jj;      // Grayscale image has 1 channel

    float r = rgbImage[rgbIndex];
    float g = rgbImage[rgbIndex + 1];
    float b = rgbImage[rgbIndex + 2];

    grayImage[grayIndex] = 0.21f * r + 0.71f * g + 0.07f * b;
  }
}

wbImage_t rgb2gray(wbImage_t inputImage) {
  const int imageWidth = wbImage_getWidth(inputImage);
  const int imageHeight = wbImage_getHeight(inputImage);

  const int imageChannels =
      wbImage_getChannels(inputImage); // For this lab the value is always 3
  float *hostInputImageData = wbImage_getData(inputImage);

  // Since the image is monochromatic, it only contains one channel
  wbImage_t outputImage = wbImage_new(imageWidth, imageHeight, 1);
  float *hostOutputImageData = wbImage_getData(outputImage);

  for (int ii = 0; ii < imageHeight; ii++) {
    for (int jj = 0; jj < imageWidth; jj++) {
      int rgbIndex = (ii * imageWidth + jj) * imageChannels;
      int grayIndex = ii * imageWidth + jj;

      float r = hostInputImageData[rgbIndex];
      float g = hostInputImageData[rgbIndex + 1];
      float b = hostInputImageData[rgbIndex + 2];

      hostOutputImageData[grayIndex] = 0.21f * r + 0.71f * g + 0.07f * b;
    }
  }

  float *out, *in;

  cudaMalloc((void **)&out, imageWidth * imageHeight * sizeof(float));
  cudaMalloc((void **)&in,
             imageWidth * imageHeight * imageChannels * sizeof(float));

  // Copy the source image to the device
  cudaMemcpy(in, hostInputImageData,
             imageWidth * imageHeight * imageChannels * sizeof(float),
             cudaMemcpyHostToDevice);
  cudaMemcpy(out, hostOutputImageData, imageWidth * imageHeight * sizeof(float),
             cudaMemcpyHostToDevice);

  // Execute the kernel
  dim3 dimGrid((imageWidth + TILE_WIDTH - 1) / TILE_WIDTH,
               (imageHeight + TILE_WIDTH - 1) / TILE_WIDTH, 1);
  dim3 dimBlock(TILE_WIDTH, TILE_WIDTH, 1);
  rgb2gray_cuda<<<dimGrid, dimBlock>>>(out, in, imageWidth, imageHeight);

  // Copy the result to the main memory
  cudaMemcpy(hostOutputImageData, out, imageWidth * imageHeight * sizeof(float),
             cudaMemcpyDeviceToHost);

  // Free the buffers on the device
  cudaFree(out);
  cudaFree(in);

  return outputImage;
}
