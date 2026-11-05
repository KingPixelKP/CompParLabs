//
// Created by Hervé Paulino on 07/10/2020.
//

#include <container_ops.h>

#include <gtest/gtest.h>

#include <cstdlib>

using namespace std;

/**
 * Assert that the contents of two vectors are the same
 */
template<typename Container>
void expect_container_value(Container &a, typename Container::value_type value) {

    auto aptr = a.data();
    for (size_t i = 0; i < a.size(); i++)
        EXPECT_EQ(aptr[i], value);
}


TEST(VectorAdd, Size10000) {

    constexpr auto size = 10000;

    // allocate in the heap, because there may not be enough space in the stack
    const auto a = make_shared<vector<int>>(size, 1);
    const auto b = make_shared<vector<int>>(size, 2);
    shared_ptr<vector<int>> c = make_shared<vector<int>>(size);

    container_add_cuda (c, a, b);

    EXPECT_EQ(c->size(), size);
    expect_container_value(*c, (int) 3);
}