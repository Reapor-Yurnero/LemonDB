//
// Created by wenda on 10/30/18.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"


class MaxQuery : public ComplexQuery {
    static constexpr const char *qname = "MAX";

public:
    using ComplexQuery::ComplexQuery;

    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> max;

    std::mutex g_mutex;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    //bool modify() override { return false; }
};



#endif //PROJECT_MAXQUERY_H
