//
// Created by dominic on 18-11-11.
//

#ifndef PROJECT_THREADPOOL_H
#define PROJECT_THREADPOOL_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <vector>
#include <deque>
#include <functional>


class ThreadPool {
public:
    typedef std::function<void(void)> Task;

    explicit ThreadPool(long num);

    ThreadPool(const ThreadPool&) = delete;

    ThreadPool &operator=(const ThreadPool&) = delete;

    void start();

    void stop();

    void addTask(const Task& task);

    void addTask(Task &&task);

    ~ThreadPool();


private:
    long thread_num;
    bool isrunning;
    std::mutex lock;
    std::condition_variable cond;
    std::deque<Task> taskset;
    std::vector<std::shared_ptr<std::thread>> pool;

    void job();
};


#endif //PROJECT_THREADPOOL_H
