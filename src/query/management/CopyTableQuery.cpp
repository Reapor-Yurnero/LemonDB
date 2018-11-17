//
// Created by camelboat on 18-11-1.
//

#include "CopyTableQuery.h"
#include "../../db/Database.h"
#include "../../db/Table.h"

#ifdef TIMER
#include <iostream>

#endif
#include <string>
#include <deque>

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    string new_table_tmp = new_table;
    try {
        std::string tableName = this->targetTable;
        auto new_table = make_unique<Table>(new_table_tmp, db[targetTable]);
        db.registerTable(move(new_table));
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"COPY takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<SuccessMsgResult>(0);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string CopyTableQuery::toString() {
    return "QUERY = COPYTABLE TABLE " + this->targetTable + "\"";
}