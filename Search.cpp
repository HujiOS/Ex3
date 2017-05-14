using namespace std;
#include <iostream>
#include <fstream>
#include <utility>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include<stdio.h>
#include<cstdlib>
#include<iostream>
#include<string>
#include<fstream>
#include<dirent.h>
#include <cstring>

const unsigned char isFile =0x8;


class DName : public k1Base{
private:
    string _file;
public:
    DName(string filename):_file(filename){}
    ~DName(){
    }
    bool operator<(const k1Base &other) const{

        return _file < ((DName&)other)._file;
    }
    const char* get() const{
        return _file.c_str();
    }
};

class myNull : public v1Base{
public:
    myNull(){};
    ~myNull(){};
};

class WordProto : public k2Base{
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

class ContainsSubstrList : public v2Base{
private:
    bool _elem;
public:
    ContainsSubstrList(bool elem){
        _elem = elem;
    };
    ContainsSubstrList(){
        _elem = false;
    };
    ~ContainsSubstrList(){
    };

    bool get_elem() const {
        return _elem;
    }
};

class WordFinished : public k3Base{
private:
    string _word;
public:
    WordFinished(string word):_word(word){};
    ~WordFinished(){};
    bool operator<(const k3Base &other) const{
        return _word < ((WordFinished&)other)._word;
    }

    string get(){
        return _word;
    }

};

class LastCounter : public v3Base{
private:
    int _counter = 0;
public:
    LastCounter(int count):_counter(count){};
    ~LastCounter(){};

    int get(){
        return _counter;
    }
};

class myMapReduce : public MapReduceBase{
private:
    string substring;
public:
    myMapReduce(string sub)
    {
        substring = sub;
    }
    void Map(const k1Base *const key, const v1Base *const val) const override{
        DIR *pDIR;
        struct dirent *entry;
        if(pDIR=opendir(((DName*)key)->get()) ){
            while(entry = readdir(pDIR)){

                //if(entry -> d_type == isFile)
                //DONT DELETE UNTIL WE KNOW IF
                //WE WANT FILES ONLY OR ALSO FOLDERS

                if( strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
                {
                    string s(entry->d_name);
                    WordProto *word = new WordProto(s);
                    ContainsSubstrList *c;

                    c = new ContainsSubstrList(s.find(substring) != string::npos);

                    Emit2(word, c);
                }
            }
            closedir(pDIR);
        }
    };

    void Reduce(const k2Base *const key, const V2_VEC &vals) const override {
        int amnt = 0;
        for(v2Base* elem : vals)
        {
            if(((ContainsSubstrList*)elem)->get_elem())
            {
                amnt++;
            }
        }
        WordFinished * word = new WordFinished(((WordProto*)key)->get());
        LastCounter *count = new LastCounter(amnt);

        Emit3(word, count);
    }
};

int main(int argc, char* argv[]){

    char *toFind = argv[1];
    myMapReduce map(toFind);
    std::vector<std::pair<k1Base*, v1Base*>> v;

    myNull* n = new myNull();
    for (int i = 2; i < argc; ++i) {
        v.push_back(std::pair<k1Base*, v1Base*>(new DName(argv[i]), n));
    }

    std::vector<std::pair<k3Base*, v3Base*>> res = RunMapReduceFramework(map, v, 2, true);

    delete n;
    for(auto p : v)
    {
        delete p.first;
    }

    for(auto p : res)
    {
        for(int i=0; i < ((LastCounter*)p.second)->get(); ++i)
        {
            printf("%s ", ((WordFinished*)p.first) -> get());
        }
        delete p .first;
        delete p.second;
    }

    return 0;
}


