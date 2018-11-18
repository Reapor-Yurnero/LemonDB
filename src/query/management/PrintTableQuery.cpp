//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"
#include "../../db/Database.h"

#include <iostream>

constexpr const char *PrintTableQuery::qname;

QueryResult::Ptr PrintTableQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try {
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        cout << "================\n";
        cout << "TABLE = ";
        cout << table;
        cout << "================\n" << endl;
        db.addresult(this->id,make_unique<SuccessMsgResult>(qname, this->targetTable));
        db.table_locks[this->targetTable]->unlock();
        return make_unique<SuccessMsgResult>(qname, this->targetTable);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    }
}

std::string PrintTableQuery::toString() {
    return "QUERY = SHOWTABLE, Table = \"" + this->targetTable + "\"";
}