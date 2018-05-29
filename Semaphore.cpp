//
// Created by hareld10 on 5/27/18.
//

#include <iostream>
#include "Semaphore.h"

void Semaphore::down() {
    while(n_ <= 0){}
    --n_;
}

void Semaphore::up() {
    std::cerr << "HIIIIIIIII" << std::endl;
    ++n_;
}
