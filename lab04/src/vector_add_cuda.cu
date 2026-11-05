#include <container_ops.h>

#include <memory>
#include <marrow/utils/timer.hpp>

using namespace std;


int main(int argc, char* argv[]) {

    if (argc != 2) {
        std::cerr << "usage " << argv[0] << " array_size \n";
       return 1;
    }

    const auto size = std::stoi( argv[1] );

    // allocate in the heap, because there may not be enough space in the stack
    const auto a = make_shared<vector<int>>(size, 1);
    const auto b = make_shared<vector<int>>(size, 2);
    auto c = make_shared<vector<int>>(size);

    marrow::timer<> t;
    t.start();
    container_add_cuda(c, a, b);
    t.stop();

    t.output_stats(cout);
    cout << " milliseconds\n ";

    return 0;
}