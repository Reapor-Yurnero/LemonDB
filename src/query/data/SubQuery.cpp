//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "SubQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try {
        db.table_locks[this->targetTable]->lock();
        this->sub_src.reserve(this->operands.size()-2);
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SUB.)"_f
                );
            }
            else {
                if (it == this->operands.begin()) {
                    sub_victim = table.getFieldIndex(*it);
                }
                else if (it+1 != this->operands.end()) {
                    sub_src.emplace_back(table.getFieldIndex(*it));
                }
                else sub_des = table.getFieldIndex(*it);
            }
        }
        auto result = initCondition(table);

        if (result.second) {
            addTaskByPaging<SubTask>(table);
        }
        return make_unique<SuccessMsgResult>(qname);

        /*
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType res = (*it)[sub_victim];
                    for (auto srcitr = sub_src.begin();srcitr!=sub_src.end();++srcitr) {
                        res -= (*it)[*srcitr];
                    }
                    (*it)[sub_des] = res;
                    ++counter;
                }
            }
        }
         */
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SUB takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<RecordCountResult>(counter);
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



void SubTask::execute() {
    auto real_query = dynamic_cast<SubQuery *>(query);

    for (auto it = begin; it != end; ++it) {
        if (real_query->evalCondition(*it)) {
            Table::ValueType res = (*it)[real_query->sub_victim];
            for (auto srcitr = real_query->sub_src.begin();srcitr!=real_query->sub_src.end();++srcitr) {
                res -= (*it)[*srcitr];
            }
            {
                std::unique_lock<std::mutex> lock(real_query->write_mutex);
                (*it)[real_query->sub_des] = res;
            }

            ++counter;
        }
    }
    /*
    {
        std::unique_lock<std::mutex> lock(real_query->g_mutex);
        real_query->counter += this->counter;
    }
    */
    real_query->mergeAndPrint();
}

QueryResult::Ptr SubQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    //auto &table = db[this->targetTable];
    std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
    ++complete_num;
    if(complete_num < (int)concurrency_num){
        return std::make_unique<NullQueryResult>();
    }
    for(const auto &task:subTasks){
        counter += task->getCounter();
    }
    db.addresult(this->id,std::make_unique<RecordCountResult>(counter));
    db.table_locks[this->targetTable]->unlock();
    db.queries.erase(id);
    //allow the next query to go
    //std::cout<<"table lock released\n";
#ifdef TIMER
    clock_gettime(CLOCK_MONOTONIC, &ts2);
    std::cerr<<"MAX takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
    return std::make_unique<NullQueryResult>();
}

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}
