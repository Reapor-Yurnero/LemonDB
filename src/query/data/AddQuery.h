//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../Query.h"

class AddQuery : public ComplexQuery {
    static constexpr const char *qname = "ADD";

public:
    using ComplexQuery::ComplexQuery;

    std::vector<Table::FieldIndex > add_src;
    Table::FieldIndex add_des;
    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return false; }
};

#endif //PROJECT_ADDQUERY_H
