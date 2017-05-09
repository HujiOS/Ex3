//
// Created by omer on 5/8/17.
//

#include <cstdio>
#include <map>
#include <iostream>
#include <mutex>
#include <thread>
#include <semaphore.h>
#include "MapReduceFramework.h"

#define CHUNK 10
#define err_msg1 "MapReduceFramework Failure: "
#define err_msg2 "failed"

// typedefs
typedef std::pair<k2Base*, v2Base*> TEMP_ITEM;
typedef std::vector<TEMP_ITEM> TEMP_ITEMS_VEC;
typedef std::pair<k2Base*, std::vector<v2Base*>> AFTER_SHUFFLE_PAIR;
typedef std::vector<AFTER_SHUFFLE_PAIR> AFTER_SHUFFLE_VEC;

// static vars
static IN_ITEMS_VEC in_items;
static OUT_ITEMS_VEC out_items;
static unsigned long currInPos = 0;
static std::map<pthread_t, TEMP_ITEMS_VEC> temp_elem_container;
static std::vector<k2Base*, std::vector<v2Base*>> after_shuffle_vec;
static MapReduceBase baseFunc;


// mutexs
static int Emit2ContainerProtection = 0;
static bool joinEnded = false;
static sem_t ShuffleSemaphore;
static pthread_mutex_t curr_in_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * This function will pop a chunk of items (CHUNK DEFINED THERE ^^^)
 *
 */
IN_ITEMS_VEC popv1Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? currInPos - in_items.size() : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return nextChunk != 0 ? tempVec : nullptr; // nullptr will mark that the vec was ended
}

AFTER_SHUFFLE_VEC popv2Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > after_shuffle_vec.size() ? currInPos - after_shuffle_vec.size() : CHUNK;
    AFTER_SHUFFLE_VEC tempVec = AFTER_SHUFFLE_VEC(after_shuffle_vec.begin()+currInPos, after_shuffle_vec.begin()+(currInPos+nextChunk));
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return nextChunk != 0 ? tempVec : nullptr; // nullptr will mark that the vec was ended
}

void Emit2 (k2Base* k, v2Base* v){
    // make sure that exec map wont try to add elements before the container init.
    while(Emit2ContainerProtection == 0){}

    // get thread container
    std::map<pthread_t, TEMP_ITEMS_VEC>::iterator it = temp_elem_container.find(pthread_self());

    // handle container finding errors
    if(it == temp_elem_container.end()){
        std::cerr << err_msg1 << "finding container " << err_msg2 << std::endl;
        return;
    }

    // push (wake up shuffle to work!)
    it->second.push_back(TEMP_ITEM(k,v));
    sem_post(&ShuffleSemaphore); // increment semaphore.
}

void Emit3 (k3Base* k, v3Base* v){
    // just push..
    // TODO something else?
    out_items.push_back(OUT_ITEM(k,v));
}



void *ExecMap(void *args){
    IN_ITEMS_VEC getData = popv1Chunk();
    while(getData != nullptr){
        for(IN_ITEM pair : getData){
            baseFunc.Map(pair.first, pair.second);
        }
        getData = popv1Chunk();
    }
    return nullptr;
}

void *ExecReduce(void *args){
    AFTER_SHUFFLE_VEC getData = popv2Chunk();
    while(getData != nullptr){
        for(auto pair : getData){
            baseFunc.Reduce(pair.first, pair.second);
        }
        getData = popv2Chunk();
    }
    return nullptr;
}


// we should change K2,V2 to K2, Vector<V2>
void *Shuffle(void *args){
    std::map<k2Base*, std::vector<v2Base*>> tempMap;
    std::map<k2Base*, std::vector<v2Base*>>::iterator vec;
    while(!joinEnded){
        sem_wait(&ShuffleSemaphore);// the sem_wait decrement the semaphore value so we should
        sem_post(&ShuffleSemaphore); // increment it :)
        for(auto pairContainer : temp_elem_container){
            TEMP_ITEMS_VEC deleted_items;
            for(auto k2v2pair : pairContainer.second){ // second is TEMP_ITEMS_VEC
                // check if this value already exists, if it is add it and than add
                // the v2 to his vector
                // otherwise just add it
                if((vec = tempMap.find(k2v2pair.first)) == tempMap.end()){
                    tempMap.insert(std::pair<k2Base*, std::vector<v2Base*>>(k2v2pair.first, std::vector<v2Base*>()));
                    tempMap.find(k2v2pair.first)->second.push_back(k2v2pair.second);
                }
                else{
                    vec->second.push_back(k2v2pair.second);
                }
                deleted_items.push_back(k2v2pair);
                sem_wait(&ShuffleSemaphore); // decrease the value of the semaphore for each finding
            }
            // we want to delete the items that we found
            pairContainer.second.erase(deleted_items.begin(), deleted_items.end());
        }
    }
    after_shuffle_vec.insert(after_shuffle_vec.end(), tempMap.begin(), tempMap.end());
    tempMap.clear();
    return nullptr;
}

void garbageCollect(bool deletev2){
    //TODO this function..
}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
    void* ret = NULL;

    // Running Map
    // save some vars
    std::vector<pthread_t> threads;
    baseFunc = mapReduce;
    if(sem_init(&ShuffleSemaphore, 0,0)){
        std::cerr << err_msg1 << "sem_init " << err_msg2 << std::endl;
        exit(1);
    }

    // create shuffleThread (1)
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, Shuffle, NULL);

    // create map threads (N)
    for(int i=0;i<multiThreadLevel;i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecMap, NULL)){
            std::cerr << err_msg1 << "pthread_create " << err_msg2 << std::endl;
            exit(1);
        };
        temp_elem_container.insert(std::pair<pthread_t, TEMP_ITEMS_VEC>(thread,TEMP_ITEMS_VEC()));
        threads.push_back(thread);
    }
    Emit2ContainerProtection = 1; // enable emit2
    for(auto tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    // join ended
    joinEnded = true;
    if(pthread_join(shuffleThread, &ret)){
        std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
        exit(1);
    }
    threads.clear();


    /// REDUCE Starts from here.
    currInPos = 0;
    for(int i=0;i<multiThreadLevel;i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecReduce, NULL)){
            std::cerr << err_msg1 << "pthread_create " << err_msg2 << std::endl;
            exit(1);
        };
        threads.push_back(thread);
    }
    for(auto tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    garbageCollect(autoDeleteV2K2);
    return out_items;


}