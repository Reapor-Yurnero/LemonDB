//
// Created by liu on 18-10-25.
//

#include "QuitQuery.h"
#include "../../db/Database.h"

constexpr const char *QuitQuery::qname;

std::string QuitQuery::toString() {
    return "QUERY = Quit";
}

QueryResult::Ptr QuitQuery::execute() {
    auto &db = Database::getInstance();
    //db.addresult(this->id,std::make_unique<SuccessMsgResult>(qname));
    auto temp = move(db.queries[this->id]);
 db.queries.erase(this->id);
    db.exit();
    // might not reach here, but we want to keep the consistency of queries
    return std::make_unique<SuccessMsgResult>(qname);
}