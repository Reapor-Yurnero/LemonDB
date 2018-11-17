//
// Created by dominic on 18-10-29.
//

#include "DeleteQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <iostream>

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (!this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Too many operands for delete"
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try{
        auto &table = db[this->targetTable];
        if(condition.empty()){
            counter = table.clear();
            return make_unique<RecordCountResult>(counter);
        }
        auto result = initCondition(table);

        if (result.second) {
            for (auto it = table.begin(); it != table.end();++it) {
                if (this->evalCondition(*it)) {
                    table.erase_marked(it);
                    ++counter;
                } else{
                    table.loadToCache(it);
                }
            }
            table.updateByCache();
        }
/*
        if (result.second) {
            for (auto it = table.begin(); it != table.end();) {
                if (this->evalCondition(*it)) {
                    it->deleteRow();
                    ++counter;
                } else
                    ++it;
            }
        }
*/
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"DELETE takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<RecordCountResult>(counter);
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

std::string DeleteQuery::toString() {
    return "QUERY = DELETE " + this->targetTable + "\"";
}
