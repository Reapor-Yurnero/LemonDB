//
// Created by liu on 18-10-25.
//

#include "ListTableQuery.h"
#include "../../db/Database.h"

constexpr const char *ListTableQuery::qname;

QueryResult::Ptr ListTableQuery::execute() {

    Database &db = Database::getInstance();
    db.table_locks[this->targetTable]->lock();
    db.printAllTable();
    db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname));
    db.table_locks[this->targetTable]->unlock();
    db.queries.erase(id);
    return std::make_unique<SuccessMsgResult>(qname);
}

std::string ListTableQuery::toString() {
    return "QUERY = LIST";
}

