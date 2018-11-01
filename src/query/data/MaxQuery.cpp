//
// Created by wenda on 10/30/18.
//

#include "MaxQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <iostream>
#include <algorithm>

constexpr const char *MaxQuery::qname;

QueryResult::Ptr MaxQuery::execute() {
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    try {
        /*this->max.clear();*/
        this->max.reserve(this->operands.size());
        auto &table = db[this->targetTable];
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for comparison.)"_f
                );
            } else {
                this->max.emplace_back(make_pair(table.getFieldIndex(*it),Table::ValueTypeMin));
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto iter=this->max.begin();iter!=this->max.end();++iter){
                        if( (*it)[(*iter).first] > (*iter).second )
                            (*iter).second = (*it)[(*iter).first];
                    }
                }
            }
        }

        vector<Table::ValueType> max_result;
        for (unsigned int i=0;i<this->max.size();i++){
            max_result.emplace_back(this->max.at(i).second);
        }
        auto ptr = make_unique<SuccessMsgResult>(max_result);
        cout << *ptr ;
        return ptr;
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

std::string MaxQuery::toString() {
    return "QUERY = MAX " + this->targetTable + "\"";
}