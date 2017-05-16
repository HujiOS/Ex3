#include <stdio.h>
#include <map>
#include <iostream>
#include <mutex>
#include <thread>
#include <semaphore.h>
#include "MapReduceFramework.h"
#include <sys/time.h>
#include <fstream>
#include <algorithm>
#include <stdlib.h>
#include <list>

#define CHUNK 10
#define err_msg1 "MapReduceFramework Failure: "
#define err_msg2 "failed"


// typedefs

typedef std::pair<std::shared_ptr<k2Base*>, std::shared_ptr<v2Base*>> TEMP_ELEM;
typedef std::vector<TEMP_ELEM> TEMP_ITEMS_VEC;
typedef std::pair<std::shared_ptr<k2Base*>, std::vector<std::shared_ptr<v2Base*>>> AFTER_SHUFFLE_PAIR;
typedef std::pair<std::shared_ptr<k2Base*>, std::vector<v2Base*>> AFTER_SHUFFLE_PAIR_NON_SHARED;
typedef std::vector<AFTER_SHUFFLE_PAIR> AFTER_SHUFFLE_VEC;
typedef std::vector<AFTER_SHUFFLE_PAIR_NON_SHARED> AFTER_SHUFFLE_VEC_NON_SHARED;
typedef unsigned int * compThread;
typedef std::vector<std::pair<k2Base*, std::vector<v2Base*>>>::iterator t2iter;
typedef std::pair<compThread, pthread_mutex_t> CONTAINER_MUTEX;


// static vars
IN_ITEMS_VEC in_items;
OUT_ITEMS_VEC out_items;
static unsigned long currInPos = 0;
std::map<compThread, TEMP_ITEMS_VEC> temp_elem_container;
std::map<compThread, pthread_mutex_t> containerLocks;
AFTER_SHUFFLE_VEC after_shuffle_vec;
static struct timeval s,e;
static std::fstream logf;
static int semVal = 0;


// mutexs
static int Emit2ContainerProtection = 0;
static bool joinEnded = false;
static sem_t ShuffleSemaphore;
static pthread_mutex_t curr_in_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t alloc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t shuffle_mutex = PTHREAD_MUTEX_INITIALIZER;


/*
 * This function will pop a chunk of items (CHUNK DEFINED THERE ^^^)
 *
 */
IN_ITEMS_VEC popv1Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? in_items.size() - currInPos : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return tempVec; // nullptr will mark that the vec was ended
}

AFTER_SHUFFLE_VEC_NON_SHARED popv2Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > after_shuffle_vec.size() ? after_shuffle_vec.size() - currInPos : CHUNK;
    auto it = (after_shuffle_vec.begin() + currInPos);
    AFTER_SHUFFLE_VEC_NON_SHARED afterTempVec;
    for(it ; it != after_shuffle_vec.begin() +(currInPos+nextChunk) ; ++it){
        std::vector<v2Base*> vt;
        for(auto &its : it->second){
            vt.push_back(*its);
        }
        if(vt.size() == 0){
            continue;
        }
        afterTempVec.push_back(std::make_pair(it->first, vt));
    }
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return afterTempVec; // nullptr will mark that the vec was ended
}
static int emits = 0;

void Emit2 (k2Base* k, v2Base* v){
    // make sure that exec map wont try to add elements before the container init.
    while(Emit2ContainerProtection == 0){}

    // get thread container
    std::map<compThread, TEMP_ITEMS_VEC>::iterator it = temp_elem_container.find((compThread)pthread_self());
    // handle container finding errors
    if(it == temp_elem_container.end()){
        std::cerr << err_msg1 << "finding container " << err_msg2 << std::endl;
        return;
    }


    pthread_mutex_lock(&containerLocks[(compThread)pthread_self()]);
    temp_elem_container[(compThread)pthread_self()].push_back
            (TEMP_ELEM(std::make_pair(std::make_shared<k2Base*>(k),std::make_shared<v2Base*>(v))));
    pthread_mutex_unlock(&containerLocks[(compThread)pthread_self()]);
}

void Emit3 (k3Base* k, v3Base* v){
    pthread_mutex_lock(&alloc_mutex);
    out_items.push_back(OUT_ITEM(k,v));
    pthread_mutex_unlock(&alloc_mutex);
}

void startMeasuringTime()
{
    if(gettimeofday(&s, NULL) != 0)
    {
        std::cerr << err_msg1 << "gettimeofday " << err_msg2 << std::endl;
    }
}

void printTime(std::string s, double diff)
{
    pthread_mutex_lock(&log_mutex);
    logf << s << " took " << diff << " ns\n";
    pthread_mutex_unlock(&log_mutex);
}

void openLogFile(int num)
{
    pthread_mutex_lock(&log_mutex);
    logf.open(".MapReduceFramework.log" ,std::fstream::in | std::fstream::out | std::fstream::app);
    if(logf.fail())
    {
        std::cerr << err_msg1 << "file open " << err_msg2 << std::endl;
        exit(1);
    }
    logf << "RunMapReduceFramework started with ";
    logf << num << " threads\n";
    pthread_mutex_unlock(&log_mutex);
}

void closeLogFile()
{
    pthread_mutex_lock(&log_mutex);
    logf << "RunMapReduceFramework finished\n";
    logf.close();
    pthread_mutex_unlock(&log_mutex);
}

void writeCreation(std::string type, bool creation)
{
    std::string action = creation ? "created " : "terminated ";
    time_t t = time(0);   // get time now
    struct tm * now = localtime(&t);

    pthread_mutex_lock(&log_mutex);
    logf << "Thread " << type + " " << creation;
    logf << "[" << now->tm_mday << "."
         << (now->tm_mon + 1) << "."
         <<  (now->tm_year + 1900) << " "
         <<  now -> tm_hour << ":" << now -> tm_min << ":"
         << now -> tm_sec << "]\n";
    pthread_mutex_unlock(&log_mutex);
}

double timeElapsed()
{
    double difference;
    if(gettimeofday(&e, NULL) != 0)
    {
        std::cerr << err_msg1 << "gettimeofday " << err_msg2 << std::endl;
    }
    difference = (e.tv_sec - s.tv_sec) * 1.0e9;
    difference += (e.tv_usec - s.tv_sec) * 1.0e3;

    return difference;
}

void *ExecMap(void *args){
    writeCreation("ExecMap", true);
    MapReduceBase *baseFunc = (MapReduceBase *)args;
    IN_ITEMS_VEC getData = popv1Chunk();
    while(getData.size() != 0){
        for(auto &pair : getData){
            baseFunc->Map(pair.first, pair.second);
        }
        getData = popv1Chunk();
    }
    writeCreation("ExecMap", false);
    return nullptr;
}

void *ExecReduce(void *args){
    writeCreation("ExecReduce", true);
    MapReduceBase *baseFunc = (MapReduceBase *) args;
    AFTER_SHUFFLE_VEC_NON_SHARED getData = popv2Chunk();
    while(getData.size() != 0){
        for(auto &iter : getData){
            baseFunc->Reduce(*iter.first, iter.second);
        }
        getData.clear();
        getData = popv2Chunk();
    }
    writeCreation("ExecReduce", false);
    return nullptr;
}

bool containersClear(){
    for(auto pairContainer = temp_elem_container.begin(); pairContainer != temp_elem_container.end(); pairContainer++){
        if(pairContainer->second.size() != 0){
            return false;
        }
    }
    return true;

}

template < class T >
bool operator==( const std::shared_ptr<T>& lhs, const std::shared_ptr<T>& rhs ) noexcept{
    return !(*(lhs.get()) < *(rhs.get())) && !(*(rhs.get()) < *(lhs.get()));
};

//bool operator==( const k2Base* lhs, const k2Base* rhs) {
//    return !(*lhs < *rhs) && !(*rhs < *lhs);
//};

struct my_cmp
{
    bool operator() (std::shared_ptr<k2Base*> const a, std::shared_ptr<k2Base*> const b)
    {
        // TODO fix this comparator.. (somehow :\) everything else should work.
        k2Base* aa = *a.get();
        k2Base* bb = *b.get();
        if(!(*aa < *bb) && !(*bb < *aa)){
            std::cout << "SUCCESSS";
        }
        std::cout<<std::endl;
        return !(*aa < *bb) && !(*bb < *aa);
    }
};

// we should change K2,V2 to K2, Vector<V2>
void *Shuffle(void *args){
    writeCreation("Shuffle", true);
    std::map<std::shared_ptr<k2Base*>, std::vector<std::shared_ptr<v2Base*>>, my_cmp> tempMap;
    std::vector<int> posses;
    int count = 0;
    while(!joinEnded || !containersClear()){
		while(containersClear() && !joinEnded);
        for(auto &pairContainer : temp_elem_container){
            pthread_mutex_lock(&containerLocks[pairContainer.first]);
            for(auto &k2v2pair : pairContainer.second){
                if(tempMap.find(k2v2pair.first) == tempMap.end()){
                    tempMap.insert(std::make_pair(k2v2pair.first, std::vector<std::shared_ptr<v2Base*>>()));
                }
                tempMap[k2v2pair.first].push_back(std::make_shared<v2Base*>(*k2v2pair.second));
                posses.push_back(count++);
            }
            std::vector<int>::iterator iter = posses.end();
            while(iter != posses.begin()){
                pairContainer.second.erase(pairContainer.second.begin() + *iter);
                iter--;
            }
            pthread_mutex_unlock(&containerLocks[pairContainer.first]);
            posses.clear();
            count = 0;
        }
    }
    for(auto &elem : tempMap){
        after_shuffle_vec.push_back(AFTER_SHUFFLE_PAIR(elem.first, elem.second));
    }
    tempMap.clear();
    writeCreation("Shuffle", false);
    return nullptr;
}

void deleteRemainsV2K2(bool deletev2){
    if(!deletev2)
    {
        return;
    }

    for(auto &i : after_shuffle_vec)
    {
        for(auto &value: i.second) {
            delete *value;
        }
    }


}


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
    void* ret = NULL;
    openLogFile(multiThreadLevel);
    in_items = itemsVec;
    // Running Map
    // save some vars
    std::vector<pthread_t> threads;
    if(sem_init(&ShuffleSemaphore, 0,0)){
        std::cerr << err_msg1 << "sem_init " << err_msg2 << std::endl;
        exit(1);
    }

    // create shuffleThread (1)
    // create map threads (N)
    startMeasuringTime();
    for(int i=0;i<multiThreadLevel;i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecMap, (void *)&mapReduce)){
            std::cerr << err_msg1 << "pthread_create " << err_msg2 << std::endl;
            exit(1);
        };
        temp_elem_container.insert(std::pair<compThread, TEMP_ITEMS_VEC>((compThread)thread,TEMP_ITEMS_VEC()));
        containerLocks.insert(CONTAINER_MUTEX((compThread)thread,PTHREAD_MUTEX_INITIALIZER));
        threads.push_back(thread);
    }

    Emit2ContainerProtection = 1; // enable emit2
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, Shuffle, NULL);
    for(auto &tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    // we are sending this post in special cases that the
    // join ended

    joinEnded = true;
    if(pthread_join(shuffleThread, &ret)){
        std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
        exit(1);
    }
    threads.clear();

    double diff = timeElapsed();
    printTime("Map and Shuffle", diff);
    startMeasuringTime();

    /// REDUCE Starts from here.
    currInPos = 0;
    for(int i=0;i < multiThreadLevel; i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecReduce, (void *)&mapReduce)){
            std::cerr << err_msg1 << "pthread_create " << err_msg2 << std::endl;
            exit(1);
        };
        threads.push_back(thread);
    }
    for(auto &tthread : threads){
        //TODO verify that the thread exists.. (or block it until we join)
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    threads.clear();
    diff = timeElapsed();
    printTime("Reduce", diff);
    deleteRemainsV2K2(autoDeleteV2K2);
    std::sort(out_items.begin(), out_items.end(), [](OUT_ITEM a, OUT_ITEM b) {
        return *(a.first) < *(b.first); });
    closeLogFile();
    std::cout << out_items.size() << " The size of the out items" << std::endl;
    return out_items;
}