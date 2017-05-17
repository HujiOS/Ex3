//
// Created by Omer on 17/05/2017.
//
#include <pthread.h>
#include <semaphore.h>
#include <iostream>

int main(){
    sem_t semi;
    int val = 0;
    sem_init(&semi, 0,10);
    sem_getvalue(&semi, &val);
    while(true){
        std::cout << "Sem Post " << val << std::endl;
        sem_wait(&semi);
        sem_getvalue(&semi, &val);
    }
}