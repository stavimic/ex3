//
// Created by hareld10 on 5/27/18.
//

#include <iostream>
#include "Semaphore.h"

void Semaphore::down() {
    std::cout<<"Semaphor down!, n = " << "   ---   " <<n_ << std::endl;

    while(n_ <= 0){}

    std::cout<<"Semaphor down!, n = "<<n_ << std::endl;

    n_ --;
}

void Semaphore::up() {
    std::cout<<"Semaphor up!" << std::endl;
    n_++;
    std::cout<<"n = "<<n_ << std::endl;

}
