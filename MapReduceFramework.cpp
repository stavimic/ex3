#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <pthread.h>
#include <algorithm>    // std::sort
#include <mutex>




//======================= Constants ======================= //




//======================================================== //
std::once_flag shuffled_flag;

struct ThreadContext {
    int threadID;  // ID of the current thread
    Barrier* barrier;  // Barrier to join the threads
    std::atomic<int>* atomic_counter;

    const MapReduceClient* client;  //Given client
    const InputVec *inputPairs;  // Input vector
    std::vector<IntermediateVec*>* intermediatePairs;  // Intermediary vector
    std::vector<IntermediateVec*>* shuffledPairs;  // Shuffled vector
    InputVec *myValues;  // Values given to the current threads
    bool startedShuffle;  // Did we start shuffling yet
    bool finishedShuffle;  // Did we finish shuffling
    OutputVec* output_vec;  // Given vector to emit the output
    pthread_mutex_t* mutex;
};

void emit2 (K2* key, V2* value, void* context){
    auto * tc = (ThreadContext*) context;4
//    tc->intermediatePairs->push_back(std::pair<K2*, V2*>(key, value));
    auto * tc = (ThreadContext*) context;

    // Lock mutex and push the pair to the intermediate vector:
    pthread_mutex_lock((tc->mutex));

    ((*(tc->intermediatePairs))[tc->threadID])->push_back(
            std::pair<K2*, V2*>(key, value));

    pthread_mutex_unlock((tc->mutex));

}


void emit3 (K3* key, V3* value, void* context)
{
    auto * tc = (ThreadContext*) context;

    // Lock mutex and push the pair to the output vector:
    pthread_mutex_lock((tc->mutex));
    (tc->output_vec)->push_back(std::pair<K3*, V3*>(key, value));
    pthread_mutex_unlock((tc->mutex));
}


void shuffle(void* context){
    auto * tc = (ThreadContext*) context;
    K2* cur_key = nullptr;

    auto num_of_vectors = static_cast<int>((*(tc->intermediatePairs)).size());

    while(true){
        for(auto intermediate_vec: (*(tc->intermediatePairs))){
            auto iter = intermediate_vec->rbegin();
            if(iter->first){

            }
        }

    }

}



void* foo(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;


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
    for(auto pair : *(tc->myValues)) {
        (tc->client)->map(pair.first, pair.second, tc);
    }

    std::sort(tc->intermediatePairs->begin(), tc->intermediatePairs->end());
    tc->barrier->barrier();

    std::call_once(shuffled_flag, []()
    {
        shuffle();
    });


}
    




void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);


    // Create the vector of IntermediateVecs, one for each thread
    auto inter_vec = new std::vector<IntermediateVec*>();
    for(int i = 0; i < multiThreadLevel; i++)
    {
        inter_vec->push_back(new IntermediateVec);
    }

    // Vector of all the shuffled pairs:
    auto shuffledPairs = new std::vector<IntermediateVec*>();

    // Initialize contexts
    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] =
                {
                        i,
                        &barrier,
                        &atomic_counter,
                        &client,
                       &inputVec,
                       inter_vec,
                       shuffledPairs,
                        new InputVec,
                        false,
                        false,
                        &outputVec,
                        &mutex
                };
    }

    //Initialize threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, nullptr, foo, contexts + i);
    }


    //Wait for threads to finish
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], nullptr);
    }

    std::cerr << "Finish runMapReduce";
}


