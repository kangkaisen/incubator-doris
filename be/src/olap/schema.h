// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_SRC_OLAP_SCHEMA_H
#define DORIS_BE_SRC_OLAP_SCHEMA_H

#include <vector>

#include "olap/aggregate_func.h"
#include "olap/field.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/descriptors.h"

namespace doris {

class Schema {
public:
    Schema(const TabletSchema& schema) {
        int offset = 0;
        _num_key_columns = 0;
        for (int i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            Field* filed = FieldFactory::create(column);
            filed->set_offset(offset);
            offset += filed->size() + 1; // 1 for null byte
            if (column.is_key()) {
                _num_key_columns++;
            }
            _cols.push_back(filed);
        }
    }

    int compare(const char* left , const char* right) const {
        for (size_t i = 0; i < _num_key_columns; ++i) {
            size_t offset = _cols[i]->get_offset();
            int comp = _cols[i]->cmp((char*)left + offset, (char*)right + offset);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    void ingest_raw_value(int index, char* dest, void* src, Arena* arena) {
        _cols[index]->consume(dest, src, arena);
    }

    void aggregate(const char* left, const char* right, Arena* arena) const {
        for (size_t i = _num_key_columns; i < _cols.size(); ++i) {
            size_t offset = _cols[i]->get_offset();
            _cols[i]->aggregate((char*)left + offset, (char*)right + offset, arena);
        }
    }

    void finalize(const char* data, Arena* arena) const {
        for (size_t i = _num_key_columns; i < _cols.size(); ++i) {
            size_t offset = _cols[i]->get_offset();
            _cols[i]->finalize((char*)data + offset , arena);
        }
    }

    int get_col_offset(int index) const {
        return _cols[index]->get_offset();
    }
    size_t get_col_size(int index) const {
        return _cols[index]->size();
    }

    bool is_null(int index, const char* row) const {
        return _cols[index]->is_null((char*)row);
    }

    void set_null(int index, char*row) {
        _cols[index]->set_null(row);
    }

    void set_not_null(int index, char*row) {
        _cols[index]->set_not_null(row);
    }

    size_t schema_size() {
        size_t size = _cols.size();
        for (auto col : _cols) {
            size += col->size();
        }
        return size;
    }
private:
    std::vector<Field*> _cols;
    size_t _num_key_columns;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_H
