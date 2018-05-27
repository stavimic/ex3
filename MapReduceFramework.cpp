#include <atomic>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"




//======================= Constants ======================= //

void emit2 (K2* key, V2* value, void* context){
   
}


void emit3 (K3* key, V3* value, void* context){}

//======================================================== //

void runMapReduceFramework(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel) {


    struct ThreadContext {

        int threadID;
        Barrier *barrier;
        std::atomic<int> *atomic_counter;

        std::vector<std::pair<K1 *, V1 *>> *inputPairs;
        std::vector<std::pair<K2 *, V2 *>> *intermediatePairs;

    };


    void emit2(K2 *key, V2 *value, void *context) {}
    void emit3(K3 *key, V3 *value, void *context) {}


    void *foo(void *arg) {

    }


    void runMapReduceFramework(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
                               int multiThreadLevel) {

        pthread_t threads[multiThreadLevel];
        ThreadContext contexts[multiThreadLevel];
        Barrier barrier(multiThreadLevel);
        std::atomic<int> atomic_counter(0);

        for (int i = 0; i < multiThreadLevel; ++i) {
            contexts[i] = {i, &barrier, &atomic_counter, nullptr, nullptr};
        }


    }

}
