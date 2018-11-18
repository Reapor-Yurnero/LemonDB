//
// Created by camelboat on 18-11-1.
//

#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include "../Query.h"
#include <vector>
#define TIMER

class SumQuery : public ConcurrentQuery {
    static constexpr const char *qname = "SUM";
#ifdef TIMER
    struct timespec ts1, ts2;
#endif

public:
    using ConcurrentQuery::ConcurrentQuery;
//    using ComplexQuery::ComplexQuery;
    std::vector<std::pair<Table::FieldIndex, Table::ValueType> > field_sum;

    std::mutex g_mutex;

    QueryResult::Ptr execute() override;

    QueryResult::Ptr mergeAndPrint() override;

    std::string toString() override;

    friend class SumTask;

    //bool modify() override { return false; }
};

class SumTask : public Task {
public:
    using Task::Task;
    std::vector<std::pair<Table::FieldIndex,Table::ValueType>> local_sum;
    void execute() override;
};

#endif //PROJECT_SUMQUERY_H
