//
// Created by camelboat on 18-11-1.
//

#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include "../Query.h"
#include <vector>

class SumQuery : public ComplexQuery {
    static constexpr const char *qname = "SUM";
    std::vector<std::pair<Table::FieldIndex, Table::ValueType> > field_sum;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //PROJECT_SUMQUERY_H
