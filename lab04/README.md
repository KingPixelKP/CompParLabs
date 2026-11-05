# Image manipulation in CUDA 

## Code Structure

- datasets: set of input and output files for the exercises you will have to solve.
- include: C++ header files
- src: C++ source files
- test: test files
- third_party: third party software, namely _googletest_ for testing and _libwb_, for image reading/writing.

## Installation

### Own Computer

If you have an NVIDIA graphics card, you may use your own computer to develop and run the project.
The software requirements are:
- C++ compiler
- CUDA toolkit

### Developing and Running on the DI cluster

If, instead of developing and running on your own machine, you need (or prefer) to use the DI cluster,
please read the instructions on Piazza.


## Running the Examples

For the vector_add initial example, you may run either the `src/vector_add` application or the 
`test/vector_add_cuda_test` test directly from your IDE.
The application times how long it takes to add two vectors together (of a specified dimension) which must be 
provided as argument.

If you prefer to run them from the command line, go to your _build_ folder and run

```bash
./src/vector_add 10000 # vector dimension parameter
```

or
```bash
./test/cuda_tests`
```

## Exercises

### Colour Image to Grayscale
Convert an RGB image into a grayscale image. For each pixel, the input is an RGB triple of
float values that you must convert into a single float grayscale intensity value.
Read the exercise sheet for more details

###  Image Blurring
Implement an efficient image blurring algorithm for an input image.
Read the exercise sheet for more details

