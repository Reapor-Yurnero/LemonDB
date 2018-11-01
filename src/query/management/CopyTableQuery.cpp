//
// Created by camelboat on 18-11-1.
//

#include "CopyTableQuery.h"
#include "../../db/Database.h"
#include "../../db/Table.h"

#include <iostream>
#include <string>
#include <deque>

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    string new_table_tmp = new_table;
    try {
        std::string tableName = this->targetTable;
        auto new_table = make_unique<Table>(new_table_tmp, db[targetTable]);
        db.registerTable(move(new_table));
//        db.printAllTable();

        return make_unique<SuccessMsgResult>(0);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string CopyTableQuery::toString() {
    return "QUERY = COPYTABLE TABLE " + this->targetTable + "\"";
}