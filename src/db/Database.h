//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_H
#define PROJECT_DB_H

#include "../threadmanage/ThreadPool.h"
#include "Table.h"

#include <memory>
#include <unordered_map>
#include <mutex>

// open timer for all queries
// #define TIMER

class Database {
private:
    /**
     * A unique pointer to the global database object
     */
    static std::unique_ptr<Database> instance;

    /**
     * The map of tableName -> table unique ptr
     */
    std::unordered_map<std::string, Table::Ptr> tables;

    /**
     * The map of fileName -> tableName
     */
    std::unordered_map<std::string, std::string> fileTableNameMap;

    /**
     * The map to store multithread results
     */
    std::unordered_map<int, QueryResult::Ptr > queryresults;

    /**
     * The default constructor is made private for singleton instance
     */
    Database() = default;

    /**
     * The threadpool for single query
     */

public:
    static std::unordered_map<std::string,std::mutex> table_locks;

    void addresult(int id,QueryResult::Ptr queryresult){
        this->queryresults.emplace(id,std::move(queryresult));
    }

    void testDuplicate(const std::string &tableName);

    Table &registerTable(Table::Ptr &&table);

    void dropTable(const std::string &tableName);

    //for truncate
    void truncateTable(const std::string &tableName);

    void printAllTable();

    Table &operator[](const std::string &tableName);

    const Table &operator[](const std::string &tableName) const;

    Database &operator=(const Database &) = delete;

    Database &operator=(Database &&) = delete;

    Database(const Database &) = delete;

    Database(Database &&) = delete;

    ~Database() = default;

    static Database &getInstance();

    void updateFileTableName(const std::string &fileName, const std::string &tableName);

    std::string getFileTableName(const std::string &fileName);

    std::string loadTableNameFromStream(std::istream &is, std::string source = "");

    /**
     * Load a table from an input stream (i.e., a file)
     * @param is
     * @param source
     * @return reference of loaded table
     */
    Table &loadTableContentFromStream(std::istream &is, std::string source = "");

    template<class RealTask>
    void addspecialTask(Database &db, Table *table = nullptr){
        ThreadPool &threadPool = ThreadPool::getPool();
        auto newTask = std::unique_ptr<RealTask>(new RealTask());
        auto newTaskPtr = newTask.get();
        threadPool.addTask(newTaskPtr);
    }

    void exit();
};


#endif //PROJECT_DB_H
