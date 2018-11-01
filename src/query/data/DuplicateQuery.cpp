//
// Created by camelboat on 18-10-31.
//

#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

//#include <algorithm>
//#include <iostream>

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute() {
    using namespace std;
    if(!this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Too many operands for duplicate"
        );
    Database &db = Database::getInstance();
    Table::SizeType  counter = 0;
    try{
        auto &table = db[this->targetTable];
        auto result = initCondition(table);
//        cout << "first is: " << result.first << endl;
//        cout << "second is: " << result.second << endl;
//        cout << "table field size is: " << table.field().size() << endl;
//        cout << "table size is: " << table.size() << endl;
        if (result.second) {
            unsigned long tmp = 0;
            unsigned long current_size = table.size();
            for (auto it = table.begin(); it != table.end() && tmp < current_size; it++) {
                if (this->evalCondition(*it)) {
                    vector<Table::ValueType> data;
                    data.reserve(table.field().size());
                    for ( unsigned long i = 0; i < table.field().size();i++ ) {
                        data.emplace_back(it->get(i));
//                        cout << it->get(i) << endl;
                    }
//                    cout << "hello" << endl;
                    string key = it->key() + "_copy";
                    if (table[key] == nullptr)
                    {
//                        cout << "new key is: " << key << endl;
                        table.insertByIndex(key, move(data));
                        counter++;
                    }
                }
//                cout << "tmp = " << tmp << endl;
                tmp++;
            }
        }
        return make_unique<RecordCountResult>(counter);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string DuplicateQuery::toString() {
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}

