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

    const MapReduceClient* client;

    std::vector<std::pair<K1*, V1*>> *inputPairs;
    std::vector<std::pair<K1*, V1*>> *myValues;

    std::vector<std::pair<K2*, V2*>> *intermediatePairs;

};

void emit2 (K2* key, V2* value, void* context){
    auto * tc = (ThreadContext*) context;
    tc->intermediatePairs->push_back(std::pair(key, value));

}
void emit3 (K3* key, V3* value, void* context){}




void* foo(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    tc->myValues = new std::vector<std::pair<K1*, V1*>>;

    bool flag = true;
    //Retrieve the next input element:
    while (flag)
    {
        int nextIndex = (*(tc->atomic_counter))++;
        if (nextIndex >= (*(tc->inputPairs)).size())
        {
            flag = false;
            break;

        }
        std::pair<K1*, V1*> nextPair = (*(tc->inputPairs))[nextIndex];
        (tc->myValues)->push_back(nextPair);  // Add the next pair to tc's input data
    }

    // Finished with the input processing, now perform the map phase:







}


void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);

    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, &barrier, &atomic_counter, &client , nullptr ,nullptr, nullptr};
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, nullptr, foo, contexts + i);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], nullptr);
    }

    std::cerr << "Finish runMapReduce";
}


