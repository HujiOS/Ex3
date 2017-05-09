//
// Created by omer on 5/8/17.
//

#include <cstdio>
#include <map>
#include <iostream>
#include <mutex>
#include <thread>
#include "MapReduceFramework.h"

#define CHUNK 10
typedef std::pair<k2Base*, v2Base*> TEMP_ITEM;
typedef std::vector<TEMP_ITEM> TEMP_ITEMS_VEC;

static IN_ITEMS_VEC in_items;
static unsigned long currInPos = 0;
static std::map<pthread_t, TEMP_ITEMS_VEC> temp_elem_container;
static std::vector<k2Base*, std::vector<v2Base*>> after_shuffle_vec;
static MapReduceBase baseFunc;


// mutexs
static int Emit2ContainerProtection = 0;
static bool joinEnded = false;
static unsigned int Emit2Size = 0;
static pthread_mutex_t ShuffleMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ShuffleAdd;
static pthread_mutex_t Emit2Mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t curr_in_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * This function will pop a chunk of items (CHUNK DEFINED THERE ^^^)
 *
 */
IN_ITEMS_VEC popChunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? currInPos - in_items.size() : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
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
        std::cerr << "error: cant find vector for thread number " << pthread_self();
        return;
    }

    // push (wake up shuffle to work!)
    pthread_mutex_lock(&ShuffleMutex);
    it->second.push_back(TEMP_ITEM(k,v));
    Emit2Size++;
    pthread_cond_signal(&ShuffleAdd);
    pthread_mutex_unlock(&ShuffleMutex);
}

void *ExecMap(void *args){
    IN_ITEMS_VEC getData = popChunk();
    while(getData != nullptr){
        for(IN_ITEM pair : getData){
            baseFunc.Map(pair.first, pair.second);
        }
        getData = popChunk();
    }
    return nullptr;
}
// we should change K2,V2 to K2, Vector<V2>
void *Shuffle(void *args){
    std::map<k2Base*, std::vector<v2Base*>> tempMap;
    std::map<k2Base*, std::vector<v2Base*>>::iterator vec;
    while(!joinEnded){
        pthread_mutex_lock(&ShuffleMutex);
        while(Emit2Size == 0){
            pthread_cond_wait(&ShuffleAdd, &ShuffleMutex);
        }
        for(auto pairContainer : temp_elem_container){
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
                Emit2Size--;
            }
        }
        after_shuffle_vec.insert(after_shuffle_vec.end(), tempMap.begin(), tempMap.end());
        tempMap.clear();
        pthread_mutex_unlock(&ShuffleMutex);
    }
    return nullptr;
}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

    // Running Map

    // save some static vars
    std::vector<pthread_t> threads;
    baseFunc = mapReduce;
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, Shuffle, NULL);
    for(int i=0;i<multiThreadLevel;i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecMap, NULL)){
            std::cerr << "Cant create thread #"<<i<<std::endl;
        };
        temp_elem_container.insert(std::pair<pthread_t, TEMP_ITEMS_VEC>(thread,TEMP_ITEMS_VEC()));
        threads.push_back(thread);
    }
    Emit2ContainerProtection = 1; // enable emit2
    void* ret = NULL;
    for(auto tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << "Cant join tthread #" << tthread << std::endl;;
        } // we dont use this ret value.
    }
    // join ended
    joinEnded = true;
    if(pthread_join(shuffleThread, &ret)){
        std::cerr << "Cant join tthread #" << shuffleThread << "Shuffle Thread" << std::endl;;
    }
    threads.clear();
    /// REDUCE Starts from here.

    // Running Reduce


    return nullptr;


}