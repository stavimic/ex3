#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <pthread.h>
#include <algorithm>    // std::sort
#include <mutex>
#include <semaphore.h>
//#include "Semaphore.h"
#include <semaphore.h>




//======================= Constants ======================= //

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};

class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};
class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;
};


int sem_value = 0;
std::mutex mtx;
bool debug = true;

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
    bool finishedShuffle;  // Did we finish shuffling
    OutputVec* output_vec;  // Given vector to emit the output
    pthread_mutex_t* mutex;
    sem_t* semi;

};

void emit2 (K2* key, V2* value, void* context)
{
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

bool comp(const std::pair< const K2*, const V2 *> &firstPair,
          const std::pair< const K2*, const V2 *> &secondPair)
{
    return *firstPair.first < *secondPair.first;
}

void shuffle(void* context){
    auto * tc = (ThreadContext*) context;
    bool first_iter = true;
    // While there are still keys to reduce:

    std::cout<< "How Many vectors in intemediatePairs" << tc->intermediatePairs->size() << std::endl;
    while(!(tc->intermediatePairs->empty()))
    {
        auto vectors_iter = tc->intermediatePairs->begin(); // get iterator on the vectors
        auto vec_iter = (*vectors_iter)->rbegin(); // get iterator on the first vector

        while((vec_iter == (*vectors_iter)->rend()))
        {
            vectors_iter = (tc->intermediatePairs)->erase(vectors_iter);
            vec_iter = (*vectors_iter)->rbegin(); // get iterator on the first vector

            if (tc->intermediatePairs->empty())
            {
                tc->finishedShuffle = true;
                return;
            }
        }

        auto to_pop = *vec_iter; // get the pair from the cur_vector
        auto cur_pair = std::pair<K2*, V2*>(to_pop.first, to_pop.second); // build new pair
        auto cur_key = cur_pair.first;
        bool is_key_alone = true;

//        (*vectors_iter)->pop_back();  // delete pair from vector
//        vectors_iter++; // Move on to next vector of pairs
//        (((tc->intermediatePairs)[0])).pop_back();
        tc->intermediatePairs->front()->pop_back();
        // Create new vectors to hold the values of cur_key :
        auto vec_to_push = new IntermediateVec;

        // Push the new vector to the shuffled pairs vector:
        pthread_mutex_lock((tc->mutex));
        vec_to_push->push_back(cur_pair);
        tc->shuffledPairs->push_back(vec_to_push);
        pthread_mutex_unlock((tc->mutex));

        if (vectors_iter ==  tc->intermediatePairs->end())
        {
            vectors_iter = tc->intermediatePairs->begin();

        }
//        first_iter = true;
        // Iterate over the remaining pairs:
        while(vectors_iter !=  tc->intermediatePairs->end())
        {

            vec_iter = (*vectors_iter)->rbegin();  // End of the current vector
            if(first_iter) {
                vec_iter++;
                first_iter = false;
            }
            // Iterate over the vector until finding the same of smaller value:
            while(vec_iter != (*vectors_iter)->rend())
            {
                auto next_pair = *vec_iter;
                if(*next_pair.first < *cur_key)
                {
                    break;
                }
                if(*cur_key < *next_pair.first)
                {
                    vec_iter++;
                    continue;
                }
                // case equal
                auto to_push = std::pair<K2*, V2*>(next_pair.first, next_pair.second);
                pthread_mutex_lock((tc->mutex));

//                (*vectors_iter)->pop_back();

                auto iter = (*vectors_iter)->begin();
                while(*iter < *vec_iter || *vec_iter < *iter)
                {
                    iter++;
                }

                vec_iter ++;
                (*vectors_iter)->erase(iter);
                tc->shuffledPairs->back()->push_back(to_push);
                sem_post((tc->semi));
                is_key_alone = false;
                pthread_mutex_unlock((tc->mutex));
                continue;
            }

            if ((*vectors_iter)->empty())
            {
                // delete the empty vector and get the next position:
                vectors_iter = (tc->intermediatePairs)->erase(vectors_iter);
                continue;
            }
            ++vectors_iter;
        }
        if (is_key_alone)
        {
            std::cout<< "alone key" << std::endl;

            sem_post((tc->semi));
        }
    }
    tc->finishedShuffle = true;
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
        pthread_mutex_lock((tc->mutex));
        std::pair<K1*, V1*> nextPair = (*(tc->inputPairs))[nextIndex];
        (tc->myValues)->push_back(nextPair);  // Add the next pair to tc's input data
        pthread_mutex_unlock((tc->mutex));
    }

    // Finished with the input processing, now perform the map phase:
    for(auto pair : *(tc->myValues)) {
        (tc->client)->map(pair.first, pair.second, tc);
    }

    // Sort the vector in the threadID cell:
    auto toSort = (*(tc->intermediatePairs))[tc->threadID];
    std::sort(toSort->begin(), toSort->end(), comp);
    tc->barrier->barrier();

    if(debug) {
        mtx.lock();
        std::cerr << "ThreadID Before Shuffle " << tc->threadID << std::endl;
        std::cerr << "Size:  " << (*(tc->intermediatePairs))[tc->threadID]->size() << std::endl;
        for (auto vec: *(*(tc->intermediatePairs))[tc->threadID]) {
            char c = ((const KChar *) vec.first)->c;
            int count = ((const VCount *) vec.second)->count;
            std::cerr << "The character " << c << " appeared " << count << " time%s" << std::endl;
        }
        std::flush(std::cerr);
        mtx.unlock();
    }

    std::call_once(shuffled_flag, [&tc]()
    {
        shuffle(tc);
    });

    tc->barrier->barrier();
    std::cerr << "Size of shuffled:  " << (tc->shuffledPairs)->size() << std::endl;

    if(debug) {
        mtx.lock();
        std::cerr << "Size:  " << (*(tc->shuffledPairs))[0]->size() << std::endl;
        for (auto vec: *(*(tc->shuffledPairs))[0]) {
            char c = ((const KChar *) vec.first)->c;
            int count = ((const VCount *) vec.second)->count;
            std::cerr << "The character " << c << " appeared " << count << " time%s" << std::endl;
        }
        std::flush(std::cerr);
        mtx.unlock();
    }

    while((!(tc->finishedShuffle)) || (!tc->shuffledPairs->empty()))
    {
        sem_wait((tc->semi)); // Wait until there is an available shuffled vector to reduce
        auto to_reduce = tc->shuffledPairs->back();  // Get the last shuffled vector
        tc->shuffledPairs->pop_back();  // Delete the last shuffled vector
        (tc->client)->reduce(to_reduce, tc);
    }
}

void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{

    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);
    sem_t* sem = new sem_t;
    sem_init(sem, 0, 0);


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
                        &outputVec,
                        &mutex,
                        sem
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

    std::cerr << "Finish runMapReduce"<<std::endl;
}


