//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "AddQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>
#ifdef TIMER
#include <iostream>

#endif

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try {
        this->add_src.reserve(this->operands.size()-1);
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for ADD.)"_f
                );
            }
            else {
                if (it+1 != this->operands.end()) {
                    add_src.emplace_back(table.getFieldIndex(*it));
                }
                else add_des = table.getFieldIndex(*it);
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType sum = 0;
                    for (auto srcitr = add_src.begin();srcitr!=add_src.end();++srcitr) {
                        sum += (*it)[*srcitr];
                    }
                    (*it)[add_des] = sum;
                    ++counter;
                }
            }
        }
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"ADD takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        return make_unique<RecordCountResult>(counter);
    }
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string AddQuery::toString() {
    return "QUERY = ADD " + this->targetTable + "\"";
}
