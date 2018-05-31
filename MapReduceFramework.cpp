#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <pthread.h>
#include <algorithm>    // std::sort
#include <mutex>
#include <semaphore.h>








///////////////////////////



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



/////////////////////////


struct ThreadContext {
    int threadID;  // ID of the current thread
    Barrier* barrier;  // Barrier to join the threads
    std::atomic<int>* atomic_counter;

    const MapReduceClient* client;  //Given client
    const InputVec *inputPairs;  // Input vector
    std::vector<IntermediateVec*>* intermediatePairs;  // Intermediary vector
    std::vector<IntermediateVec*>* shuffledPairs;  // Shuffled vector
    InputVec *myValues;  // Values given to the current threads
    OutputVec* output_vec;  // Given vector to emit the output
    pthread_mutex_t* mutex;
    sem_t* semi;
    std::atomic<int>* index_counter;
    std::atomic<int>* finishedShuffling; // Did we finish shuffling
    std::once_flag* shuffled_flag;  // One-time Flag
    std::atomic<int>* reduced_counter;
    int num_of_threads;





};


std::once_flag p_flag;



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

int shuffled_counter=0;


void shuffle(void* context){
    auto * tc = (ThreadContext*) context;
    bool first_iter = true;
    // While there are still keys to reduce:
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
                (*(tc->finishedShuffling))++;
                return;
            }
        }

        auto to_pop = *vec_iter; // get the pair from the cur_vector
        auto cur_pair = std::pair<K2*, V2*>(to_pop.first, to_pop.second); // build new pair
        auto cur_key = cur_pair.first;

        (tc->intermediatePairs->front())->pop_back();
        // Create new vectors to hold the values of cur_key :
        auto vec_to_push = new IntermediateVec;

        // Push the new vector to the shuffled pairs vector:
        pthread_mutex_lock((tc->mutex));
        vec_to_push->push_back(cur_pair);
        tc->shuffledPairs->push_back(vec_to_push);
        pthread_mutex_unlock((tc->mutex));

        while(vectors_iter !=  tc->intermediatePairs->end())
        {

            vec_iter = (*vectors_iter)->rbegin();  // End of the current vector

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
                auto iter = (*vectors_iter)->begin();
                while(*iter < *vec_iter || *vec_iter < *iter)
                {
                    iter++;
                }

                vec_iter ++;
                (*vectors_iter)->erase(iter);
                tc->shuffledPairs->back()->push_back(to_push);
                pthread_mutex_unlock((tc->mutex));
            }

            if ((*vectors_iter)->empty())
            {
                // delete the empty vector and get the next position:
                vectors_iter = (tc->intermediatePairs)->erase(vectors_iter);
                continue;
            }
            ++vectors_iter;

        }
        sem_post((tc->semi));
    }
    (*(tc->finishedShuffling))++;
    for(int i = 0;  i < tc->num_of_threads; i++)
    {
        sem_post((tc->semi));
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


    std::call_once(*(tc->shuffled_flag), [&tc]()
    {
        shuffle(tc);
    });


//    std::call_once(p_flag, [&tc]()
//    {
//        if(true) {
//            for(int i = 0; i < 21; i++) {
//
//                std::cerr << "Size:  " << (*(tc->shuffledPairs))[i]->size() << std::endl;
//                for (auto vec: *(*(tc->shuffledPairs))[i]) {
//                    char c = ((const KChar *) vec.first)->c;
//                    int count = ((const VCount *) vec.second)->count;
//                    std::cerr << "The character " << c << " appeared " << count << " time%s" << std::endl;
//                }
//                std::flush(std::cerr);
//
//            }
//        }
//    });


    int index = 0;
    while(true)
    {
        sem_wait((tc->semi)); // Wait until there is an available shuffled vector to reduce
        index = (*(tc->index_counter))++;
        if (index >= (*(tc->shuffledPairs)).size())
        {
            break;
        }


        IntermediateVec* to_reduce = (*(tc->shuffledPairs))[index];  // Get the next shuffled vector
        (tc->client)->reduce(to_reduce, tc);
    }
    return nullptr;
}


void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{
    std::once_flag shuffled_flag;
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(0);
    std::atomic<int> index_counter(0);
    std::atomic<int> reduced_counter(0);
    std::atomic<int> shuffle_boolean(0);
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);
    pthread_mutex_t mutex_r(PTHREAD_MUTEX_INITIALIZER);
    bool* finished_shuffling = new bool(false);
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
                        &outputVec,
                        &mutex,
                        sem,
                        &index_counter,
                        &shuffle_boolean,
                        &shuffled_flag,
                        &reduced_counter,
                        multiThreadLevel
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

//    std::cerr << "Finish runMapReduce"<<std::endl;
}


