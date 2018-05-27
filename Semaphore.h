//
// Created by hareld10 on 5/27/18.
//

#ifndef EX3_SEMAPHORE_H
#define EX3_SEMAPHORE_H


#include <atomic>

class Semaphore {

    explicit Semaphore(int n):n_(n){}
    void down();
    void up();

private:
    std::atomic<int> n_;
};


#endif //EX3_SEMAPHORE_H
