#include <atomic>
#include <iostream>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <pthread.h>
#include <algorithm>    // std::sort
#include <mutex>
#include <semaphore.h>



struct ThreadContext {
    int threadID;  // ID of the current thread
    Barrier* barrier;  // Barrier to join the threads
    std::atomic<unsigned int>* atomic_counter;

    const MapReduceClient* client;  //Given client
    const InputVec *inputPairs;  // Input vector
    std::vector<IntermediateVec*>* intermediatePairs;  // Intermediary vector
    std::vector<IntermediateVec*>* shuffledPairs;  // Shuffled vector
    InputVec *myValues;  // Values given to the current threads
    OutputVec* output_vec;  // Given vector to emit the output
    pthread_mutex_t* mutex;
    sem_t* semi;
    std::atomic<unsigned int>* index_counter;
    int num_of_threads;
    std::atomic<unsigned int>* one_flag;  // Flag to determine who shuffles

};

/// push values to intermediatePairs
/// \param key
/// \param value
/// \param context
void emit2 (K2* key, V2* value, void* context)
{
    auto * tc = (ThreadContext*) context;

    // Lock mutex and push the pair to the intermediate vector:
    pthread_mutex_lock((tc->mutex));
    ((*(tc->intermediatePairs))[tc->threadID])->push_back(
            std::pair<K2*, V2*>(key, value));
    pthread_mutex_unlock((tc->mutex));

}

/// push values to final vector
/// \param key
/// \param value
/// \param context
void emit3 (K3* key, V3* value, void* context)
{
    auto * tc = (ThreadContext*) context;
    // Lock mutex and push the pair to the output vector:
//    pthread_mutex_lock((tc->mutex));
    (tc->output_vec)->push_back(std::pair<K3*, V3*>(key, value));
//    pthread_mutex_unlock((tc->mutex));
}

/// function used as comperator between 2 pairs
/// \param firstPair
/// \param secondPair
/// \return ans base on the first elements in pair
bool comp(const std::pair< const K2*, const V2 *> &firstPair,
          const std::pair< const K2*, const V2 *> &secondPair)
{
    return *firstPair.first < *secondPair.first;
}



void shuffle(void* context)
{
    auto * tc = (ThreadContext*) context;
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
                return;
            }
        }

        // Find the maximal value in the intermediate pairs:
        IntermediateVec* maxmimum_vector = *(vectors_iter);  // The max vector
        std::pair<K2*, V2*> max_pair = (maxmimum_vector->back());  // The currently maximal pair
        vectors_iter++; // Go to the next vector of pairs

        while(vectors_iter != tc->intermediatePairs->end())
        {
            vec_iter = (*vectors_iter)->rbegin();
            while((vec_iter == (*vectors_iter)->rend()) & ((tc->intermediatePairs)->size() != 1))
            {
                vectors_iter++;
                if(vectors_iter == (tc->intermediatePairs->end()))
                {
                    break;
                }
                vec_iter = (*vectors_iter)->rbegin();
            }
            if ((tc->intermediatePairs)->size() == 1)
            {
                break;  // There is only one vector in the intermediatePairs, so we are already holding the max key
            }
            if(vectors_iter == tc->intermediatePairs->end())
            {
                break;
            }
            std::pair<K2*, V2*> cur_pair = ((*(vectors_iter))->back());
            if (*((max_pair).first) < (*(cur_pair).first))
            {
                maxmimum_vector = *vectors_iter;  // This is the vector that holds the maximum key so far
            }
            vectors_iter ++;

        }

        std::pair<K2*, V2*> max = (maxmimum_vector->back());  // The maximal pair
        // so now, the maximal key in the intermediate pairs is pointed to by max_pair
        std::pair<K2*, V2*> cur_pair = std::pair<K2*, V2*>(max.first, max.second); // build new pair
        auto cur_key = cur_pair.first;

        pthread_mutex_lock((tc->mutex));
        (maxmimum_vector)->pop_back();  // Delete the chosen pair
        pthread_mutex_unlock((tc->mutex));


        // Create vector to hold the values of the maximum key, and push it to the shuffled pairs vector:
        IntermediateVec* vec_to_push = new IntermediateVec;
        pthread_mutex_lock((tc->mutex));
        vec_to_push->push_back(cur_pair);
        (tc->shuffledPairs)->push_back(vec_to_push);
        pthread_mutex_unlock((tc->mutex));

        vectors_iter = (tc->intermediatePairs)->begin(); // get iterator over the vectors
        while(vectors_iter !=  tc->intermediatePairs->end())
        {
            vec_iter = (*vectors_iter)->rbegin();  // End of the current vector
            // Iterate over the vector until finding the same of smaller value:
            while(vec_iter != (*vectors_iter)->rend())
            {
                auto next_pair = *vec_iter;
                if(*next_pair.first < *cur_key)
                {
                    // nothing to find in this vector - GetOut!
                    break;
                }

                // case equal
                std::pair<K2*, V2*> to_push = std::pair<K2*, V2*>(next_pair.first, next_pair.second);
                pthread_mutex_lock((tc->mutex));
                (*vectors_iter)->pop_back();  // Delete this pair
                (tc->shuffledPairs->back())->push_back(to_push);
                vec_iter++;
                pthread_mutex_unlock((tc->mutex));
            }

            if ((*vectors_iter)->empty())
            {
                // delete the empty vector and get the next position:
                pthread_mutex_lock((tc->mutex));
                vectors_iter = (tc->intermediatePairs)->erase(vectors_iter);
                pthread_mutex_unlock((tc->mutex));

                continue;
            }
            ++vectors_iter;

        }
        sem_post((tc->semi));
    }
    /// finished shuffle so boost the semaphore to ensure all threads will be at the stage where
    /// they need to reduce
    for(int i = 0;  i < (tc->num_of_threads); i++)
    {
        sem_post((tc->semi));
    }

}


/// each thread runs this function
/// \param arg the context of the thread
/// \return
void* startRoutine(void *arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    unsigned int index;

    //Retrieve the next input element:
    while (true)
    {
        unsigned int nextIndex = (*(tc->atomic_counter))++;
        if (nextIndex >= (*(tc->inputPairs)).size())
        {
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
    // wait all threads will come here to start shuffle
    tc->barrier->barrier();
    auto shuffle_index = (*(tc->one_flag))++;
    // shuffled called only once - first thread to catch
    if(shuffle_index == 0)
    {
        shuffle(tc);
    }

    // loop will run until all reduces have finished
    while(true)
    {
        sem_wait((tc->semi)); // Wait until there is an available shuffled vector to reduce
        index = (*(tc->index_counter))++;
        pthread_mutex_lock((tc->mutex));
        if (index >= (*(tc->shuffledPairs)).size())
        {
            pthread_mutex_unlock((tc->mutex));
            break;
        }
        IntermediateVec* to_reduce = (*(tc->shuffledPairs))[index];  // Get the next shuffled vector
        (tc->client)->reduce(to_reduce, tc);
        pthread_mutex_unlock((tc->mutex));
    }
    return nullptr;
}


/// main function to run the whole Framework
/// \param client according to instructions
/// \param inputVec input values
/// \param outputVec where the output values will be at the end
/// \param multiThreadLevel num of threads to use
void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel) {

    std::once_flag shuffled_flag;  // to ensure shuffle runs only once
    pthread_t threads[multiThreadLevel]; // array of the threads
    ThreadContext contexts[multiThreadLevel];  // array of the contexts
    Barrier barrier(multiThreadLevel);  // barrier shared between all
    std::atomic<unsigned int> atomic_counter(0);  // in order to retrieve the input safely
    std::atomic<unsigned int> index_counter(0);  // count amount of reduced operations
    std::atomic<unsigned int> one_flag(0);  // once flag, to determines who shuffles the input
    pthread_mutex_t mutex(PTHREAD_MUTEX_INITIALIZER);
    sem_t *sem = new sem_t;  // Semaphore
    sem_init(sem, 0, 0);

    // Create the vector of IntermediateVecs, one for each thread
    auto inter_vec = new std::vector<IntermediateVec *>();
    for (int i = 0; i < multiThreadLevel; i++) {
        inter_vec->push_back(new IntermediateVec);
    }

    // Vector of all the shuffled pairs:
    auto shuffledPairs = new std::vector<IntermediateVec *>();


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
                        multiThreadLevel,
                        &one_flag
                };
    }

    //Initialize threads
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_create(threads + i, nullptr, startRoutine, contexts + i);
    }


    //Wait for threads to finish
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(threads[i], nullptr);
    }


    /// free up memory
    delete sem;
    inter_vec->clear();
    delete inter_vec;
    shuffledPairs->clear();
    delete shuffledPairs;

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        delete (contexts[i].myValues);

    }
}


