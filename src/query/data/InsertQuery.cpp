//
// Created by liu on 18-10-25.
//

#include "InsertQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *InsertQuery::qname;

QueryResult::Ptr InsertQuery::execute() {
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
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        auto &key = this->operands.front();
        vector<Table::ValueType> data;
        data.reserve(this->operands.size() - 1);
        for (auto it = ++this->operands.begin(); it != this->operands.end(); ++it) {
            data.emplace_back(strtol(it->c_str(), nullptr, 10));
        }
        table.insertByIndex(key, move(data));
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"INSERT takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname, targetTable));
        db.table_locks[this->targetTable]->unlock();
        db.queries.erase(id);
        return std::make_unique<SuccessMsgResult>(qname, targetTable);
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

std::string InsertQuery::toString() {
    return "QUERY = INSERT " + this->targetTable + "\"";
}
