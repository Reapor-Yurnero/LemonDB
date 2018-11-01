//
// Created by dominic on 18-10-29.
//

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <map>

#include <iostream>

constexpr const char *SelectQuery::qname;

QueryResult::Ptr SelectQuery::execute() {
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    try{
        auto &table = db[this->targetTable];
        if (this->operands[0] != "KEY")
            return make_unique<ErrorMsgResult>(
                    qname, "The beginning field is not KEY"
            );
        auto result = initCondition(table);
        map<Table::KeyType, vector<Table::ValueType > > rowData;
        if (result.second) {
            for (auto table_it = table.begin(); table_it != table.end(); ++table_it) {
                if (this->evalCondition(*table_it)) {
                    // todo: optimize tmp vector
                    vector<Table::ValueType> datum;
                    for (auto field_it = ++this->operands.begin(); field_it != this->operands.end(); ++field_it) {
                        datum.emplace_back((*table_it)[*field_it]);
                    }
                    rowData.insert(make_pair(table_it->key(), datum));
                }
            }
        }
        return std::make_unique<AnswerMsgResult>(rowData);
    } catch (const TableNameNotFound &e) {
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

std::string SelectQuery::toString() {
    return "QUERY = SELECT " + this->targetTable + "\"";
}
