using namespace std;
#include <iostream>
#include <fstream>
#include <utility>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
typedef pair<WordProto, OneList> levelTwoPair;
typedef pair<WordProto, vector<OneList>> ShufflePair;
typedef pair<WordFinished, LastCounter> levelThreePair;


class fname : k1Base{
private:
    string _file;
public:
    fname(string filename):_file(filename){}
    ~fname(){
    }
    bool operator<(const fname &other){
        return _file < other._file;
    }
    string get(){
        return _file;
    }
};

class myNull : v1Base{
public:
    myNull(){};
    ~myNull(){};
};

class WordProto : k2Base{
private:
    string _word;
public:
    WordProto(string word):_word(word){};
    ~WordProto(){};
    bool operator<(const WordProto &other){
        return _word < other._word;
    }
    string get(){
        return _word;
    }
};

class OneList : v2Base{
private:
    int elem = 1;
public:
    OneList(){
    };
    ~OneList(){
    };
};

class WordFinished : k3Base{
private:
    string _word;
public:
    WordFinished(string word):_word(word){};
    ~WordFinished(){};
    bool operator<(const WordProto &other){
        return _word < other._word;
    }
    string get(){
        return _word;
    }
};

class LastCounter : v3Base{
private:
    int coutner = 0;
public:
    LastCounter(){};
    ~LastCounter(){};
    void add(){
        coutner++;
    }
    int get(){
        return coutner;
    }
};


class myMapReduce : MapReduceBase{
    vector<levelTwoPair> Map(const fname *const key, const myNull *const val){
        vector<levelTwoPair> wlist;
        ifstream ifs(key->get());
        string line;
        while(getline(ifs, line)){
            wlist.add(levelTwoPair(WordProto(line), OneList()));
        }
        return wlist;
    };

    vector<levelThreePair> Reduce(const k2Base *const key, const V2_VEC &vals){

    }
};



