#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"




//======================= Constants ======================= //




//======================================================== //


struct ThreadContext {

    int threadID;
    Barrier* barrier;
    std::atomic<int>* atomic_counter;

    std::vector<std::pair<K1*, V1*>> *inputPairs;
    std::vector<std::pair<K2*, V2*>> *intermediatePairs;

};

void emit2 (K2* key, V2* value, void* context){
    auto * tc = (ThreadContext*) context;
    tc->intermediatePairs->push_back(std::pair(key, value));

}
void emit3 (K3* key, V3* value, void* context){}




void* foo(void* arg)
{
    bool flag = true;


}


void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);

    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, &barrier, &atomic_counter, nullptr, nullptr};
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, nullptr, foo, contexts + i);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], nullptr);
    }

    std::cerr << "Finish runMapReduce";
}


