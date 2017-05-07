using namespace std;
#include <iostream>
#include <fstream>
#include <utility>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include<stdio.h>
#include<cstdlib>
#include<iostream>
#include<string.h>
#include<fstream>
#include<dirent.h>



typedef pair<WordProto, OneList> levelTwoPair;
typedef pair<WordProto, vector<OneList>> ShufflePair;
typedef pair<WordFinished, LastCounter> levelThreePair;

const unsigned char isFile =0x8;


class DName : k1Base{
private:
    string _file;
public:
    DName(string filename):_file(filename){}
    ~DName(){
    }
    bool operator<(const DName &other){
        return _file < other._file;
    }
    const char* get() const{
        return _file.c_str();
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
    bool operator<(const k2Base &other) const override{
        return _word < ((WordProto&)other)._word;
    }
    const char* get() const{
        return _word.c_str();
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
    bool operator<(const k3Base &other){
        return _word < ((WordFinished&)other)._word;
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
    vector<levelTwoPair> Map(const k1Base *const key, const v1Base *const val){
        vector<levelTwoPair> ret;


        DIR *pDIR;
        struct dirent *entry;
        if(pDIR=opendir(((DName*)key)->get()) ){
            while(entry = readdir(pDIR)){
                if(entry -> d_type == isFile)           // necessary? do we want folders also? if so we have the root thing commented below
                {
                    //if( strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) //DONT DELETE UNTIL WE KNOW IF
                    //WE WANT FILES ONLY OR ALSO FOLDERS
                    cout << entry->d_name << "\n";
                }
            }
            closedir(pDIR);
        }
    };

    vector<levelThreePair> Reduce(const k2Base *const key, const V2_VEC &vals){

    }
};



