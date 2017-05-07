using namespace std;
#include <iostream>
#include <fstream>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

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
};

class

map<String, vector<int>> mapFoo(const string *const key, const string *const val){
    map<string, vector<int>> myNewVec;
    ifstream ifs("my-data.txt");
    string line;
    while(getline(ifs, line)){
        if(myNewVec.find(line) == m.end()){
            // not found
            myNewVec.insert(line, vector<int>(1));

        }
        else{
            // found.
            myNewVec.find(line).add(1);
        }
    }
};