//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"

class CountQuery : public ComplexQuery {
    static constexpr const char *qname = "DELETE";
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

#endif //PROJECT_COUNTQUERY_H
