#include <container_ops.h>


int main(int argc, char **argv) {
    int deviceCount;

    cudaGetDeviceCount(&deviceCount);

    for (int dev = 0; dev < deviceCount; dev++) {
        cudaDeviceProp deviceProp;

        cudaGetDeviceProperties(&deviceProp, dev);

        if (dev == 0) {
            if (deviceProp.major == 9999 && deviceProp.minor == 9999) {
                LOG("No CUDA GPU has been detected");
                return -1;
            } else if (deviceCount == 1) {

                LOG("There is 1 device supporting CUDA");
            } else {
                LOG("There are " << deviceCount << " devices supporting CUDA");
            }
        }

        LOG("Device " << dev << " name: " << deviceProp.name);
        LOG(" Computational Capabilities: " << deviceProp.major << "." << deviceProp.minor);
        LOG(" Maximum global memory size: " << deviceProp.totalGlobalMem);
        LOG(" Maximum constant memory size: " << deviceProp.totalConstMem);
        LOG(" Maximum shared memory size per block: " << deviceProp.sharedMemPerBlock);
        LOG(" Maximum block dimensions: " << deviceProp.maxThreadsDim[0]
                 << " x " << deviceProp.maxThreadsDim[1]
                 << " x " << deviceProp.maxThreadsDim[2]);
        LOG(" Maximum grid dimensions: " << deviceProp.maxGridSize[0]
                 << " x " << deviceProp.maxGridSize[1]
                 << " x " << deviceProp.maxGridSize[2]);
        LOG(" Warp size: " << deviceProp.warpSize);

    }

    return 0;
}

