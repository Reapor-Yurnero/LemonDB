//
// Created by dominic on 18-10-29.
//

#include "CountQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *CountQuery::qname;

QueryResult::Ptr CountQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (!this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Too many operands for count"
        );
    Database &db = Database::getInstance();
    try{
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];

        auto result = initCondition(table);
        if (result.second) {
//            for (auto it = table.begin(); it != table.end();++it) {
//                if (this->evalCondition(*it))
//                    ++counter;
//            }
            addTaskByPaging<CountTask>(table);
        }
        else{
            db.table_locks[this->targetTable]->unlock();
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"COUNT takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<SuccessMsgResult>(qname);
    } catch (const TableNameNotFound &e) {
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

std::string CountQuery::toString() {
    return "QUERY = Count " + this->targetTable + "\"";
}

QueryResult::Ptr CountQuery::mergeAndPrint() {
    Database &db = Database::getInstance();
    {
        std::unique_lock<std::mutex> concurrentLocker(concurrentLock);
        ++complete_num;
        if(complete_num < (int)concurrency_num){
            return std::make_unique<NullQueryResult>();
        }
    }
    db.addresult(this->id,std::make_unique<AnswerMsgResult>(this->countresult));
    db.table_locks[this->targetTable]->unlock();
    return std::make_unique<NullQueryResult>();
}

void CountTask::execute() {
    auto real_query = dynamic_cast<CountQuery *>(query);
    unsigned int local_count = 0;
    for (auto table_it = begin;table_it != end;++table_it) {
        if (real_query->evalCondition(*table_it)) ++local_count;
    }
    //update the global count result
    {
        std::unique_lock<std::mutex> lock(real_query->count_mutex);
        real_query->countresult += local_count;
    }
    real_query->mergeAndPrint();
}