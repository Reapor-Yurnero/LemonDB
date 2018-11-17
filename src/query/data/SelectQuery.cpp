//
// Created by dominic on 18-10-29.
//

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <map>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *SelectQuery::qname;

QueryResult::Ptr SelectQuery::execute() {
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
    try {
        auto &table = db[this->targetTable];
        std::unique_lock<std::mutex> writeLocker(table.writeLock);
        if (this->operands[0] != "KEY")
            return make_unique<ErrorMsgResult>(
                    qname, "The beginning field is not KEY"
            );
        auto result = initCondition(table);
        map<Table::KeyType, vector<Table::ValueType *> > rowData;
        if (result.second) {
            for (auto table_it = table.begin(); table_it != table.end(); ++table_it) {
                if (this->evalCondition(*table_it)) {
                    vector<Table::ValueType *> datum;
                    datum.reserve(table.field().size());
                    for (auto field_it = ++this->operands.begin(); field_it != this->operands.end(); ++field_it) {
                        datum.emplace_back(&((*table_it)[*field_it]));
                    }
                    rowData.emplace(make_pair(table_it->key(), move(datum)));
                }
            }
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SELECT takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return std::make_unique<AnswerMsgResult>(move(rowData));
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

std::string SelectQuery::toString() {
    return "QUERY = SELECT " + this->targetTable + "\"";
}
