//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "SubQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute() {
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try {
        this->sub_src.reserve(this->operands.size()-2);
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SUB.)"_f
                );
            }
            else {
                if (it == this->operands.begin()) {
                    sub_victim = table.getFieldIndex(*it);
                }
                else if (it+1 != this->operands.end()) {
                    sub_src.emplace_back(table.getFieldIndex(*it));
                }
                else sub_des = table.getFieldIndex(*it);
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType res = (*it)[sub_victim];
                    for (auto srcitr = sub_src.begin();srcitr!=sub_src.end();++srcitr) {
                        res -= (*it)[*srcitr];
                    }
                    (*it)[sub_des] = res;
                    ++counter;
                }
            }
        }

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

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}
