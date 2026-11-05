#ifndef CADBLABS_HPP
#define CADBLABS_HPP

#include <array>
#include <vector>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>

using namespace std;

constexpr auto THREADS_PER_BLOCK = 512;

#define LOG(message) {  std::cout << "[INFO] " <<  message << std::endl; }


template <typename T, size_t Size>
ostream& operator<<(ostream& out, const array<T, Size>& a) {

    out << "[ ";
    copy(a.begin(),
         a.end(),
         ostream_iterator<T>(out, " "));
    out << "]";

    return out;
}

template <typename T>
ostream& operator<<(ostream& out, const vector<T>& a) {

    out << "[ ";
    copy(a.begin(),
         a.end(),
         ostream_iterator<T>(out, " "));
    out << "]";

    return out;

}

__global__ void add(int* out, const int* b, const int* c, const unsigned size) {
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index < size)
        out[index] = b[index] + c[index];
}


/**
 * out = b + c
 *
 * @tparam Container1
 * @tparam Container2
 * @tparam Container3
 * @param out
 * @param b
 * @param c
 */
template <typename Container1, typename Container2, typename Container3>
void container_add_cuda(shared_ptr<Container1> out, const shared_ptr<Container2> b, const shared_ptr<Container3> c) {

    const auto size = out->size();
    const auto sizeout_in_bytes = size * sizeof(typename Container1::value_type);
    const auto sizeb_in_bytes = min(sizeout_in_bytes, b->size() * sizeof(typename Container2::value_type));
    const auto sizec_in_bytes = min(sizeout_in_bytes, c->size() * sizeof(typename Container3::value_type));

    // allocate in the GPU
    int *d_out, *d_b, *d_c;
    cudaMalloc((void **)&d_out, sizeout_in_bytes);
    cudaMalloc((void **)&d_b, sizeb_in_bytes);
    cudaMalloc((void **)&d_c, sizec_in_bytes);


    // Copy to the GPU
    cudaMemcpy(d_b, b->data(), sizeb_in_bytes,  cudaMemcpyHostToDevice);
    cudaMemcpy(d_c, c->data(), sizec_in_bytes,  cudaMemcpyHostToDevice);

    // Perform the computation
    const auto nb = (size + THREADS_PER_BLOCK - 1)/THREADS_PER_BLOCK;
    add<<<nb, THREADS_PER_BLOCK>>>(d_out, d_b, d_c, size);

    // Copy the results back to the host
    cudaMemcpy(out->data(), d_out, sizec_in_bytes, cudaMemcpyDeviceToHost);

    cudaFree(d_out); cudaFree(d_b); cudaFree(d_c);
}


#endif //CADBLABS_HPP
