//
// Created by wenda on 10/30/18.
//

#include "MaxQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

#define TIMER // counter for MAX solely
#include <iostream>

#include <unistd.h>
constexpr const char *MaxQuery::qname;


void find_local_max(MaxQuery* const query, Table::Iterator begin, Table::Iterator end,
                    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_max){
    //local vector to store max in this page
    /*
    {
        std::unique_lock<std::mutex> lock(query->g_mutex);
        local_max.reserve(query->max.size());

        for(size_t i = 0; i< query->max.size() ; i++)
            local_max.emplace_back(std::make_pair(query->max[i].first,Table::ValueTypeMin));
    }

    std::cerr << "max size " << query->max.size() << "\n";
    std::cerr << "max content " << query->max[0].first << "\n";
    std::cerr << "ahh\n";
    std::cerr << local_max[0].first << " test \n\n";
    std::cerr << (*begin)[std::string("class")] << " test \n";
    */
    for (auto it = begin; it != end; ++it) {
        if (query->evalCondition(*it)) {
            for (auto iter=local_max.begin();iter!=local_max.end();++iter){
                if( (*it)[(*iter).first] > (*iter).second )
                    (*iter).second = (*it)[(*iter).first];
                //std::cerr<<(*iter).second<<" in loop!\n";
            }
        }
    }

    //std::cerr << local_max[0].second << " test2 \n\n";

    {
        std::unique_lock<std::mutex> lock(query->g_mutex);
        for (size_t i=0;i<local_max.size();i++){
            if(local_max[i].second > query->max[i].second)
                std::swap(local_max[i].second, query->max[i].second);
        }
    }

    //std::cerr << query->max[0].second << " test3 \n\n";
}


QueryResult::Ptr MaxQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    ThreadPool &threadPool = ThreadPool::getPool();
    try {
        /*this->max.clear();*/
        this->max.reserve(this->operands.size());
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for MAX.)"_f
                );
            } else {
                this->max.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMin));
            }
        }
        auto result = initCondition(table);

        /*
        if (result.second) {

            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto iter=this->max.begin();iter!=this->max.end();++iter){
                        if( (*it)[(*iter).first] > (*iter).second )
                            (*iter).second = (*it)[(*iter).first];
                    }
                }
            }
        }
        */


        /*
        if (result.second) {
            auto table_size = table.size();
            auto thread_num = std::thread::hardware_concurrency()-1;

            //std::cerr<<thread_num<< "Threadnum!\n";
            //thread_num = 7;

            std::vector<std::thread> thread_total;
            thread_total.reserve(thread_num);
            //calculate the corresponding page number
            size_t page_size = (table_size - 1)/thread_num + 1;
            size_t begin=0, end = begin+page_size;

            while(end<table.size()){
                std::thread thread(find_local_max,this,&table,table.begin()
                                 + begin, table.begin() + end, this->max);
                thread_total.emplace_back(std::move(thread));
                begin += page_size;
                end +=page_size;
            }
            //handle the last page
            std::thread thread(find_local_max,this,&table,
                               table.begin() + begin, table.end(),this->max);
            thread_total.emplace_back(std::move(thread));
            for(size_t i =0;i<thread_total.size();i++){
                thread_total[i].join();
            }
        }
        */


        if (result.second) {
            auto table_size = table.size();
            auto thread_num = std::thread::hardware_concurrency()-1;

            //calculate the corresponding page number
            if(table_size<1000) thread_num =1;
            size_t page_size = (table_size - 1)/thread_num + 1;
            size_t begin=0, end = begin+page_size;

            while(end<table_size){
                threadPool.addTask(std::bind(find_local_max,this,
                                              table.begin() + begin, table.begin() + end
                                            ,this->max));
                begin += page_size;
                end +=page_size;
            }
            //handle the last page
            std::cerr << "max content in main" << this->max[0].first << "\n";
            threadPool.addTask(std::bind(find_local_max,this,
                               table.begin() + begin, table.end()
                               ,this->max));

            //todo@ find a better way than busy wait?
            threadPool.waitfinish();
        }

        vector<Table::ValueType> max_result;
        for (unsigned int i=0;i<this->max.size();i++){
            max_result.emplace_back(this->max.at(i).second);
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"MAX takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<AnswerMsgResult>(max_result);
    }
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string MaxQuery::toString() {
    return "QUERY = MAX " + this->targetTable + "\"";
}