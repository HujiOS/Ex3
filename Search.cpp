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
    const std::string _file;
public:
    DName(string filename):_file(filename){}
    bool operator<(const k1Base &other) const{

        return _file < ((DName&)other)._file;
    }
    const std::string& get(){
        return _file;
    }
};

class myNull : public v1Base{
public:
    myNull(){};
};

class WordProto : public k2Base{
private:
    const std::string _word;
public:
    WordProto(string word):_word(word){};
    bool operator<(const k2Base &other) const override{
        return _word < ((WordProto&) other)._word;
    }
    const std::string& get(){
        return _word;
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

    bool get_elem() const {
        return _elem;
    }
};

class WordFinished : public k3Base{
private:
    const std::string _word;
public:
    WordFinished(string word):_word(word){};
//    virtual bool operator<(const k2Base& other) const
    virtual bool operator<(const k3Base &other) const{
        WordFinished ot = ((WordFinished&)other);
        return _word < ot.get();
    }
    const std::string& get(){
        return _word;
    }

};

class LastCounter : public v3Base{
private:
    int _counter = 0;
public:
    LastCounter(int count):_counter(count){};

    int get(){
        return _counter;
    }
};

class myMapReduce : public MapReduceBase{
private:
    string _substring;
public:
    myMapReduce(string sub)
    {
        _substring = sub;
    }
    virtual void Map(const k1Base *const key, const v1Base *const val) const override{
        DIR *pDIR;
        struct dirent *entry;
        if(pDIR=opendir(((DName*)key)->get().c_str()) ){
            while(entry = readdir(pDIR)){

                //if(entry -> d_type == isFile)
                //DONT DELETE UNTIL WE KNOW IF
                //WE WANT FILES ONLY OR ALSO FOLDERS

                if( strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
                {
                    string s(entry->d_name);
                    WordProto *word = new WordProto(s);
                    ContainsSubstrList *c;
                    c = s.find(_substring) !=
                                string::npos ? new ContainsSubstrList(true) : new ContainsSubstrList(false);
                    Emit2(word, c);
                }
            }
            closedir(pDIR);
        }
    };

    virtual void Reduce(const k2Base *const key, const V2_VEC &vals) const override {
        int amnt = 0;
        for(auto &elem : vals)
        {
            if(((ContainsSubstrList*)elem)->get_elem())
            {
                amnt++;
            }
        }
        if(amnt == 0){
            return;
        }
        WordFinished *word = new  WordFinished(((WordProto*)key)->get());
        LastCounter *count = new LastCounter(amnt);
        if(!word || !count){
            cout << "BAD ALLOCATION!!!!!" << endl;
            return;
        }
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
    std::cout << "deleted first batch" << std::endl;
    for(auto p : res)
    {
        for(int i=0; i < ((LastCounter*)p.second)->get(); ++i)
        {
            std::cout << ((WordFinished*)p.first) -> get() << " ";
        }
        delete p.first;
        delete p.second;
    }
    std::cout << std::endl;

    return 0;
}


