//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include "../Query.h"

class SubQuery : public ComplexQuery {
    static constexpr const char *qname = "SUB";
    std::vector<Table::FieldIndex > sub_src;
    Table::FieldIndex sub_victim, sub_des;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    bool modify() override { return false; }
};

#endif //PROJECT_SUBQUERY_H
