//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "TruncateTableQuery.h"
#include "../../db/Database.h"

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *TruncateTableQuery::qname;

QueryResult::Ptr TruncateTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    try {
        db.table_locks[this->targetTable]->lock();
        db.truncateTable(this->targetTable);
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"TRUNCATE takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname));
        db.table_locks[this->targetTable]->unlock();
        db.queries.erase(this->id);
        return make_unique<SuccessMsgResult>(qname);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string TruncateTableQuery::toString() {
    return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}