//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"

class SelectQuery : public ComplexQuery {
    static constexpr const char *qname = "SELECT";
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return false; }
};


#endif //PROJECT_SELECTQUERY_H
