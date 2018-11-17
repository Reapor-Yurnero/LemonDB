//
// Created by liu on 18-10-25.
//

#include "LoadTableQuery.h"
#include "../../db/Database.h"

#include <fstream>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *LoadTableQuery::qname;

QueryResult::Ptr LoadTableQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    try {
        ifstream infile(this->fileName);
        if (!infile.is_open()) {
            return make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
        }
        db.loadTableFromStream(infile, this->fileName);
        infile.close();
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"LOAD takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<SuccessMsgResult>(qname, targetTable);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string LoadTableQuery::toString() {
    return "QUERY = Load TABLE, FILE = \"" + this->fileName + "\"";
}
