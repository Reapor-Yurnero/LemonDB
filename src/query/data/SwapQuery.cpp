//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "SwapQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *SwapQuery::qname;

QueryResult::Ptr SwapQuery::execute() {
    using namespace std;
    if (this->operands.size() != 2)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try {
        auto &table = db[this->targetTable];
        int mark = 0;
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SWAP.)"_f
                );
            }
            else {
                if (mark == 0) swapA = table.getFieldIndex(*it);
                else swapB = table.getFieldIndex(*it);
                mark++;
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType temp = (*it)[swapA];
                    (*it)[swapA] = (*it)[swapB];
                    (*it)[swapB] = temp;
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

std::string SwapQuery::toString() {
    return "QUERY = SWAP " + this->targetTable + "\"";
}