//
// Created by omer on 5/8/17.
//

#include <cstdio>
#include <map>
#include "MapReduceFramework.h"

#define CHUNK 10
typedef std::pair<k2Base*, v2Base*> TEMP_ITEM;
typedef std::vector<TEMP_ITEM> TEMP_ITEMS_VEC;

static IN_ITEMS_VEC in_items;
static unsigned long currInPos = 0;
static int currInProtection = 0; // seg to protect the pos incrementation
static int Emit2ContainerProtection = 0; // seg to protect the pos incrementation
static std::map<pthread_t, TEMP_ITEMS_VEC> temp_elem_container;


//semaphore like programming :x
void down(){
    while(currInProtection <= 0){}
    currInProtection--;
}
void up(){
    currInProtection++;
}
/*
 * This function will pop a chunk of items (CHUNK DEFINED THERE ^^^)
 *
 */
IN_ITEMS_VEC popChunk(){
    down();
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? currInPos - in_items.size() : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
    up();
    return nextChunk != 0 ? tempVec : nullptr; // nullptr will mark that the vec was ended
}


void Emit2 (k2Base* k, v2Base* v){
    // make sure that exec map wont try to add elements before the container init.
    while(Emit2ContainerProtection == 0){}
    map<pthread_t, TEMP_ITEMS_VEC>::iterator it temp_elem_container.find(pthread_self());
    if(it == temp_elem_container.end()){
        cerr << "error: cant find vector for thread number " << pthread_self();
        return nullptr;
    }
    it->second.push_back(TEMP_ITEM(k,v));
}

