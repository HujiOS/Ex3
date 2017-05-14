#include <cstdio>
#include <map>
#include <iostream>
#include <mutex>
#include <thread>
#include <semaphore.h>
#include <stdlib.h>
#include "MapReduceFramework.h"
#include <sys/time.h>
#include <fstream>

#define CHUNK 10
#define err_msg1 "MapReduceFramework Failure: "
#define err_msg2 "failed"





// typedefs
typedef std::pair<k2Base*, v2Base*> TEMP_ITEM;
typedef std::vector<TEMP_ITEM> TEMP_ITEMS_VEC;
typedef std::pair<k2Base*, std::vector<v2Base*>> AFTER_SHUFFLE_PAIR;
typedef std::vector<AFTER_SHUFFLE_PAIR> AFTER_SHUFFLE_VEC;
typedef unsigned int * compThread;
typedef std::vector<std::pair<k2Base*, std::vector<v2Base*>>>::iterator t2iter


// static vars
static IN_ITEMS_VEC in_items;
static OUT_ITEMS_VEC out_items;
static unsigned long currInPos = 0;
static std::map<compThread, TEMP_ITEMS_VEC> temp_elem_container;
static std::vector<std::pair<k2Base*, std::vector<v2Base*>>> after_shuffle_vec;
static struct timeval s,e;
static std::fstream logf;


// mutexs
static int Emit2ContainerProtection = 0;
static bool joinEnded = false;
static sem_t ShuffleSemaphore;
static pthread_mutex_t curr_in_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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
    return tempVec; // nullptr will mark that the vec was ended
}

AFTER_SHUFFLE_VEC popv2Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > after_shuffle_vec.size() ? currInPos - after_shuffle_vec.size() : CHUNK;
    AFTER_SHUFFLE_VEC tempVec = AFTER_SHUFFLE_VEC(after_shuffle_vec.begin()+currInPos, after_shuffle_vec.begin()
                                                                                       +(currInPos+nextChunk));
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return tempVec; // nullptr will mark that the vec was ended
}

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

    // push (wake up shuffle to work!)
    it->second.push_back(TEMP_ITEM(k,v));
    sem_post(&ShuffleSemaphore); // increment semaphore.
}

void Emit3 (k3Base* k, v3Base* v){
    // just push..
    // TODO something else?
    out_items.push_back(OUT_ITEM(k,v));
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
    logf << s << ' took ' << diff << ' ns\n';
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
    logf << 'Thread ' << type + " " << creation;
    logf << '[' << now->tm_mday << '.'
         << (now->tm_mon + 1) << '.'
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
    MapReduceBase *baseFunc = (MapReduceBase *) args;
    IN_ITEMS_VEC getData = popv1Chunk();
    while(getData.size() != 0){
        for(IN_ITEM pair : getData){
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
    AFTER_SHUFFLE_VEC getData = popv2Chunk();
    while(getData.size() != 0){
        for(auto pair : getData){
            baseFunc->Reduce(pair.first, pair.second);
        }
        getData = popv2Chunk();
    }
    writeCreation("ExecReduce", false);
    return nullptr;
}


// we should change K2,V2 to K2, Vector<V2>
void *Shuffle(void *args){
    writeCreation("Shuffle", true);
    std::map<k2Base*, std::vector<v2Base*>> tempMap;
    std::map<k2Base*, std::vector<v2Base*>>::iterator vec;
    while(!joinEnded){ // TODO check if temp_elem_conainer is empty. << in this loop using sem_getvalue
        sem_wait(&ShuffleSemaphore);// the sem_wait decrement the semaphore value so we should
        sem_post(&ShuffleSemaphore); // increment it :) **I have some questions about this**
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
                else
                {
                    vec->second.push_back(k2v2pair.second);
                }
                deleted_items.push_back(k2v2pair);
                sem_wait(&ShuffleSemaphore); // decrease the value of the semaphore for each finding
            }
            // we want to delete the items that we found
            pairContainer.second.erase(deleted_items.begin(), deleted_items.end());
        }
    }
    after_shuffle_vec.assign(tempMap.begin(), tempMap.end());
    tempMap.clear();
    writeCreation("Shuffle", false);
    return nullptr;
}

void deleteRemainsV2K2(bool deletev2){
    if(!deletev2)
    {
        return;
    }

    for(t2iter i = after_shuffle_vec.begin(); i < after_shuffle_vec.end(); ++i)
    {
        for(auto value: i -> second) {
            delete value;
        }
        delete i -> first;
    }


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
    void* ret = NULL;

    openLogFile(multiThreadLevel);
    // Running Map
    // save some vars
    std::vector<pthread_t> threads;
    if(sem_init(&ShuffleSemaphore, 0,0)){
        std::cerr << err_msg1 << "sem_init " << err_msg2 << std::endl;
        exit(1);
    }

    // create shuffleThread (1)
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, Shuffle, NULL);

    // create map threads (N)
    startMeasuringTime();
    for(int i=0;i<multiThreadLevel;i++){
        pthread_t thread;
        if(pthread_create(&thread, NULL, ExecMap, (void *)&mapReduce)){
            std::cerr << err_msg1 << "pthread_create " << err_msg2 << std::endl;
            exit(1);
        };
        temp_elem_container.insert(std::pair<compThread, TEMP_ITEMS_VEC>((compThread)thread,TEMP_ITEMS_VEC()));
        threads.push_back(thread);
    }
    Emit2ContainerProtection = 1; // enable emit2
    for(auto tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    // we are sending this post in special cases that the
    sem_post(&ShuffleSemaphore);
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
    for(auto tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    diff = timeElapsed();
    printTime("Reduce", diff);

    deleteRemainsV2K2(autoDeleteV2K2);
    return out_items;
}