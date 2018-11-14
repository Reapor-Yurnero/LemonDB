//
// Created by dominic on 18-11-11.
//

#include "ThreadPool.h"

std::unique_ptr<ThreadPool> ThreadPool::threadpool = nullptr;

ThreadPool::ThreadPool(long num): thread_num(num), isrunning(false), lock(), cond() {}

ThreadPool::~ThreadPool() {
    if(isrunning)
        stop();
}

void ThreadPool::start() {
    isrunning = true;
    pool.reserve((unsigned long)thread_num);
    for(int i = 0; i < thread_num; i++){
        pool.emplace_back(std::make_shared<std::thread>(&ThreadPool::job, this));
    }
}

void ThreadPool::stop() {
    {
        std::unique_lock<std::mutex> locker(lock);
        isrunning = false;
        cond.notify_all();
    }

    for(long i = 0; i < thread_num; ++i){
        auto thread_t = pool[i];
        if(thread_t->joinable())
            thread_t->join();
    }
}

void ThreadPool::addTask(const ThreadPool::Task &task) {
    if(isrunning){
        std::unique_lock<std::mutex> locker(lock);
        taskset.emplace_back(task);
        cond.notify_one();
    }
}

void ThreadPool::addTask(ThreadPool::Task &&task) {
    if(isrunning){
        std::unique_lock<std::mutex> locker(lock);
        taskset.emplace_back(std::move(task));
        cond.notify_one();
    }
}

void ThreadPool::job() {
    while(isrunning){
        Task task;
        {
            std::unique_lock<std::mutex> locker(lock);
            if (isrunning && taskset.empty())
                cond.wait(locker);
            if (!taskset.empty()) {
                task = taskset.front();
                taskset.pop_front();
            }
        }
        if(task)
            task();
    }
}

ThreadPool &ThreadPool::initPool(long num) {
    if(ThreadPool::threadpool == nullptr) {
        threadpool = std::unique_ptr<ThreadPool>(new ThreadPool(num));
        threadpool->start();
    }
    return *threadpool;
}

ThreadPool &ThreadPool::getPool() {
    return *threadpool;
}