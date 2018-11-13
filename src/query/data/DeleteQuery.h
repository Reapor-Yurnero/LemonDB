//
// Created by dominic on 18-10-29.
//

#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include "../Query.h"

class DeleteQuery : public ComplexQuery {
    static constexpr const char *qname = "DELETE";
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    bool modify() override { return true; }
};

#endif //PROJECT_DELETEQUERY_H
