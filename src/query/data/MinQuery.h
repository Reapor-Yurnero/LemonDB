//
// Created by Reapor Yurnero on 31/10/2018.
//

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"

class MinQuery : public ComplexQuery {
    static constexpr const char *qname = "MIN";
    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> min;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    bool modify() override { return false; }
};


#endif //PROJECT_MINQUERY_H
