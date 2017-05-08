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
static std::mutex curr_in_mutex;
static int Emit2ContainerProtection = 0; // seg to protect the pos incrementation
static std::map<std::thread::id, TEMP_ITEMS_VEC> temp_elem_container;
static std::vector<k2Base*, std::vector<v2Base*>> after_shuffle_vec;

/*
 * This function will pop a chunk of items (CHUNK DEFINED THERE ^^^)
 *
 */
IN_ITEMS_VEC popChunk(){
    curr_in_mutex.lock();
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? currInPos - in_items.size() : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
    currInPos += nextChunk;
    curr_in_mutex.unlock();
    return nextChunk != 0 ? tempVec : nullptr; // nullptr will mark that the vec was ended
}

void Emit2 (k2Base* k, v2Base* v){
    // make sure that exec map wont try to add elements before the container init.
    while(Emit2ContainerProtection == 0){}
    std::map<std::thread::id, TEMP_ITEMS_VEC>::iterator it = temp_elem_container.find(std::this_thread::get_id());
    if(it == temp_elem_container.end()){
        std::cerr << "error: cant find vector for thread number " << pthread_self();
        return;
    }
    it->second.push_back(TEMP_ITEM(k,v));
}

void ExecMap(void (*map)(const k1Base *const, const v1Base *const)){
    IN_ITEMS_VEC getData = popChunk();
    while(getData != nullptr){
        for(IN_ITEM pair : getData){
            map(pair.first, pair.second);
        }
        getData = popChunk();
    }
    return;
}
// we should change K2,V2 to K2, Vector<V2>
void Shuffle(){
    std::map<k2Base*, std::vector<v2Base*>> tempMap;
    std::map<k2Base*, std::vector<v2Base*>>::iterator vec;
    for(auto pairContainer : temp_elem_container){
        for(auto k2v2pair : pairContainer.second){ // second is TEMP_ITEMS_VEC
            // check if this value already exists, if it is add it and than add
            // the v2 to his vector
            // otherwise just add it
            if((vec = tempMap.find(k2v2pair.first)) == tempMap.end()){
                tempMap.insert(std::pair<k2Base*, std::vector<v2Base*>>(k2v2pair.first,
                                                                        std::vector<v2Base*>()));
                tempMap.find(k2v2pair.first)->second.push_back(k2v2pair.second);
            }
            else{
                vec->second.push_back(k2v2pair.second);
            }
        }
    }
    after_shuffle_vec.assign(tempMap.begin(), tempMap.end());
}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

    // Running Map
    std::vector<std::thread> threads;
    for(int i=0;i<multiThreadLevel;i++){
        threads.push_back(std::thread(ExecMap, mapReduce.Map));
        temp_elem_container.insert(std::pair<std::thread::id, TEMP_ITEMS_VEC>(threads.back().get_id(),
                                                                              TEMP_ITEMS_VEC()));
    }
    Emit2ContainerProtection = 1; // start Emitting!#$!
    for(auto thread : threads){
        thread.join();
    }
    threads.clear();

    // Running Shuffle
    Shuffle();

    // Running Reduce



}