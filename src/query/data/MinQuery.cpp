//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "MinQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *MinQuery::qname;

QueryResult::Ptr MinQuery::execute() {
    // todo: optimize the data structure for comparison
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    try {
        /*this->min.clear();*/
        this->min.reserve(this->operands.size());
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for comparison.)"_f
                );
            } else {
                this->min.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMax));
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto iter = this->min.begin();iter!=this->min.end();++iter){
                        if((*it)[(*iter).first] < (*iter).second) (*iter).second = (*it)[(*iter).first];
                    }
                }
            }
        }
        // todo: optimize the return result
        vector<Table::ValueType> min_result;
        for (unsigned int i=0;i<this->min.size();i++){
            min_result.emplace_back(this->min.at(i).second);
        }
        return make_unique<SuccessMsgResult>(min_result);
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

std::string MinQuery::toString() {
    return "QUERY = MIN " + this->targetTable + "\"";
}