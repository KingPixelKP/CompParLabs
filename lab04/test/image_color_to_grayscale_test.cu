#include <wb.h>

#include <gtest/gtest.h>

wbImage_t rgb2gray(wbImage_t inputImage);

TEST(CUDA, GrayScale) {
    const char* inputImageFile = "../../datasets/ImageColorToGrayscale/0/input.ppm";
    const char* outputImageFile = "../../datasets/ImageColorToGrayscale/0/output.ppm";

    wbImage_t inputImage = wbImport(inputImageFile);
    wbImage_t resultImage = rgb2gray(inputImage);

    // Test for equality
    float *resultImageData = wbImage_getData(resultImage);
    
    wbImage_t expectedImage = wbImport(outputImageFile);
    const int imageWidth = wbImage_getWidth(expectedImage);
    const int imageHeight = wbImage_getHeight(expectedImage);
    float* expectedImageData = wbImage_getData(expectedImage);

    for (int i = 0; i < imageHeight; i++)
        for (int j = 0; j < imageWidth; j++)
            EXPECT_NEAR(resultImageData[i * imageWidth + j], expectedImageData[i * imageWidth + j], 1e-2);

    wbImage_delete(resultImage);
    wbImage_delete(inputImage);
}
