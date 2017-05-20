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


// mutexs
// used to Emit2 & Shuffle
sem_t semiSemaphore;
// used to main & Threads
static int Emit2ContainerProtection = 0;
// used to Shuffle & main
static bool joinEnded = false;
// used to Maps \ Reduce threads
static pthread_mutex_t curr_in_mutex = PTHREAD_MUTEX_INITIALIZER;
// used to write into the log
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
// used to Maps \ Reduce threads
static pthread_mutex_t alloc_mutex = PTHREAD_MUTEX_INITIALIZER;


/*
 * no so sophisticated function who iterate over the in_items container and return 10
 * elements for each request.
 */
IN_ITEMS_VEC popv1Chunk(){
    pthread_mutex_lock(&curr_in_mutex);
    size_t nextChunk = currInPos + CHUNK > in_items.size() ? in_items.size() - currInPos : CHUNK;
    IN_ITEMS_VEC tempVec = IN_ITEMS_VEC(in_items.begin()+currInPos, in_items.begin()+(currInPos+nextChunk));
    currInPos += nextChunk;
    pthread_mutex_unlock(&curr_in_mutex);
    return tempVec; // nullptr will mark that the vec was ended
}
/*
 * no so sophisticated function who iterate over the temp (after shuffle) container and return 10
 * elements for each request.
 */
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

/*
 * Emit2 predefined function getting and element from the user (after map) and insert it to the
 * temporary container.
 */
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
    sem_post(&semiSemaphore); // we dont want a fight against the shuffle so we wake him up after we
    // added the element into the array.
}
/*
 * pre-defined function (library function) get reduced element and insert it to the container.
 */
void Emit3 (k3Base* k, v3Base* v){
    pthread_mutex_lock(&alloc_mutex);
    out_items.push_back(OUT_ITEM(k,v));
    pthread_mutex_unlock(&alloc_mutex);
}

/*
 * this function will print time to the log file (current time)
 */
void printTime(std::string s, double diff)
{
    pthread_mutex_lock(&log_mutex);
    logf << s << " took " << diff << " ns\n";
    pthread_mutex_unlock(&log_mutex);
}
/*
 * this function will close the log file
 */
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
/*
 * this function will close the given log file
 */
void closeLogFile()
{
    pthread_mutex_lock(&log_mutex);
    logf << "RunMapReduceFramework finished\n";
    logf.close();
    pthread_mutex_unlock(&log_mutex);
}
/*
 * write into logger function (getting type + create \ termniate bool) and
 * will write it to the opened log file
 */
void writeCreation(std::string type, bool creation)
{
    std::string action = creation ? "created " : "terminated ";
    time_t t = time(0);   // get time now
    struct tm * now = localtime(&t);

    pthread_mutex_lock(&log_mutex);
    logf << "Thread " << type + " " << action;
    logf << "[" << now->tm_mday << "."
         << (now->tm_mon + 1) << "."
         <<  (now->tm_year + 1900) << " "
         <<  now -> tm_hour << ":" << now -> tm_min << ":"
         << now -> tm_sec << "]\n";
    pthread_mutex_unlock(&log_mutex);
}
/*
 * this function is a start function for time Elapsed it starts the counter of the time.
 */
void startMeasuringTime()
{
    if(gettimeofday(&s, NULL) != 0)
    {
        std::cerr << err_msg1 << "gettimeofday " << err_msg2 << std::endl;
    }
}
/*
 * time logger function (have start & stop) and return the difference.
 */
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

/*
 * this function will run the Map Procedure
 * it will ask for elements from popv1Chunk and map every one of the elements
 */
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
/*
 * this function will run the Reduce Procedure.
 * it will ask for eleemnts from popv2Chunk and reduce every one of them.
 */
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
/*
 * this function is checking if there is any element left in the main Emit2Container.
 */
bool containersClear(){
    for(auto pairContainer = temp_elem_container.begin(); pairContainer != temp_elem_container.end(); pairContainer++){
        if(pairContainer->second.size() != 0){
            return false;
        }
    }
    return true;

}

// this struct help us to determine equals between k2Base pointers in Map container.
struct my_cmp
{
    bool operator() (std::shared_ptr<k2Base*> const a, std::shared_ptr<k2Base*> const b)
    {
        k2Base* aa = *a.get();
        k2Base* bb = *b.get();
        return (*aa < *bb); // badass comparator
    }
};

/*
 * this function is an implementation to the MapReduce shuffle function. it iterate over the
 * elements containers and insert them to relevant map to keep them safe from monsters
 */
void *Shuffle(void *args){
    writeCreation("Shuffle", true);
    std::map<std::shared_ptr<k2Base*>, std::vector<std::shared_ptr<v2Base*>>, my_cmp> tempMap;
    std::vector<int> posses;
    int count = 0;
    sem_wait(&semiSemaphore); // we want to keep the semaphore on -1 because we dont want him to run
                            // he will be tired..
    while(!joinEnded || !containersClear()){
        for(auto &pairContainer : temp_elem_container){
            pthread_mutex_lock(&containerLocks[pairContainer.first]);
            for(auto &k2v2pair : pairContainer.second){
                if(tempMap.find(k2v2pair.first) == tempMap.end()){
                    tempMap.insert(std::make_pair(k2v2pair.first, std::vector<std::shared_ptr<v2Base*>>()));
                }
                tempMap[k2v2pair.first].push_back(std::make_shared<v2Base*>(*k2v2pair.second));
                posses.push_back(count++);
                if(count != pairContainer.second.size()){
                    sem_wait(&semiSemaphore); // we dont want to wait with mutex in our hand
                                                // so im checking if it is the last element
                                                // and if it is I will decrease the semaphore by one
                                                // outside this loop
                }
            }
            std::vector<int>::iterator iter = posses.end();
            // erase from originral container (we have mutex no worries)
            while(iter != posses.begin()){
                pairContainer.second.erase(pairContainer.second.begin() + *iter);
                iter--;
            }
            // no mutex anymore
            pthread_mutex_unlock(&containerLocks[pairContainer.first]);
            if(count != 0){
                sem_wait(&semiSemaphore); // checking if we found something
                                // AKA GOTOSLEEP!
            }
            posses.clear();
            count = 0;
        }
    }
    for(auto &elem : tempMap){
        // update after shuffle container.
        after_shuffle_vec.push_back(AFTER_SHUFFLE_PAIR(elem.first, elem.second));
    }
    tempMap.clear();
    writeCreation("Shuffle", false);
    return nullptr;
}
/*
 * this function will release (if necessary) all the k2Base, v2Base elements that allocated
 * during the run.
 */
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

/*
 * the main function of our library, kind of a main function
 * explanation inside.
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

    // Init Phase
    void* ret = NULL;
    openLogFile(multiThreadLevel);
    in_items = itemsVec;

    // Running Map
    // semaphore init (sami ha semaphore)
    std::vector<pthread_t> threads;
    if(sem_init(&semiSemaphore, 0,0)){
        std::cerr << err_msg1 << "sem_init " << err_msg2 << std::endl;
        exit(1);
    }

    startMeasuringTime();
    // running the Map procefure (+Shuffle)
    // this loop will create the threads and their mutexes
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

    // creating Shuffle Thread
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, Shuffle, NULL);

    // joining threads
    for(auto &tthread : threads){
        if(pthread_join(tthread, &ret)){
            std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
            exit(1);
        } // we dont use this ret value.
    }
    // we are sending this post in special cases that the
    // join ended

    joinEnded = true;
    sem_post(&semiSemaphore); // because semi ha semaphore was on -1 all the time we need to
    // increase his val.
    if(pthread_join(shuffleThread, &ret)){
        std::cerr << err_msg1 << "pthread_join " << err_msg2 << std::endl;
        exit(1);
    }
    threads.clear();


    // Map + Shuffle eneded, starting reducc
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

    // deleting v2k2 elements
    deleteRemainsV2K2(autoDeleteV2K2);

    // sorting data
    std::sort(out_items.begin(), out_items.end(), [](OUT_ITEM a, OUT_ITEM b) {
        return *(a.first) < *(b.first); });
    closeLogFile();

    //return items
    return out_items;
}