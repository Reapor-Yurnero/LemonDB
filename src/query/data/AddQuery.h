//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../Query.h"

class AddQuery : public ComplexQuery {
    static constexpr const char *qname = "ADD";
    std::vector<Table::FieldIndex > add_src;
    Table::FieldIndex add_des;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //PROJECT_ADDQUERY_H
