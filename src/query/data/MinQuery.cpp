//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "MinQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

// #define TIMER
#ifdef TIMER
#include <iostream>

#endif

#include <algorithm>

constexpr const char *MinQuery::qname;

/*
void find_local_min(MinQuery* const query, Table::Iterator begin, Table::Iterator end,
                    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_min){

    for (auto it = begin; it != end; ++it) {
        if (query->evalCondition(*it)) {
            for (auto iter=local_min.begin();iter!=local_min.end();++iter){
                if( (*it)[(*iter).first] < (*iter).second )
                    (*iter).second = (*it)[(*iter).first];
            }
        }
    }


    {
        std::unique_lock<std::mutex> lock(query->g_mutex);
        for (size_t i=0;i<local_min.size();i++){
            if(local_min[i].second < query->min[i].second)
                std::swap(local_min[i].second, query->min[i].second);
        }
    }
}
*/

QueryResult::Ptr MinQuery::execute() {
    // todo: optimize the data structure for comparison
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
    //ThreadPool &threadPool = ThreadPool::getPool();
    try {
        /*this->min.clear();*/
        db.table_locks[this->targetTable]->lock();
        this->min.reserve(this->operands.size());
        auto &table = db[this->targetTable];

        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for comparison.)"_f
                );
            } else {
                this->min.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMax));
            }
        }
        auto result = initCondition(table);


        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto iter = this->min.begin();iter!=this->min.end();++iter){
                        if((*it)[(*iter).first] < (*iter).second) (*iter).second = (*it)[(*iter).first];
                    }
                }
            }
        }

/*
        if (result.second) {
            auto table_size = table.size();
            auto thread_num = std::thread::hardware_concurrency()-1;

            //calculate the corresponding page number
            if(table_size<1000) thread_num =1;
            size_t page_size = (table_size - 1)/thread_num + 1;
            size_t begin=0, end = begin+page_size;

            while(end<table_size){
                threadPool.addTask(std::bind(find_local_min,this,
                                             table.begin() + begin, table.begin() + end
                        ,this->min));
                begin += page_size;
                end +=page_size;
            }
            //handle the last page
            //std::cerr << "min content in main" << this->min[0].first << "\n";
            threadPool.addTask(std::bind(find_local_min,this,
                                         table.begin() + begin, table.end()
                    ,this->min));

            //todo@ find a better way than busy wait?
            threadPool.waitfinish();
        }
*/
        // todo: optimize the return result
        vector<Table::ValueType> min_result;
        for (unsigned int i=0;i<this->min.size();i++){
            min_result.emplace_back(this->min.at(i).second);
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"MIN takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<AnswerMsgResult>(min_result);
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

std::string MinQuery::toString() {
    return "QUERY = MIN " + this->targetTable + "\"";
}