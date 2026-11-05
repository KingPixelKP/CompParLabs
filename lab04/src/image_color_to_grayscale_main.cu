#include <wb.h>

wbImage_t rgb2gray(wbImage_t inputImage);

int main(int argc, char *argv[]) {

    if (argc != 2) {
        printf ("usage %s image_file.ppm\n", argv[0]);
        return 1;
    }
    const char *inputImageFile = argv[1];

    const wbImage_t inputImage = wbImport(inputImageFile);
    wbImage_t outputImage = rgb2gray(inputImage);
    wbExport("output.ppm", outputImage);

    wbImage_delete(outputImage);
    wbImage_delete(inputImage);

    return 0;
}