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

#include "olap/memtable.h"

#include "common/bitmap.h"
#include "olap/hll.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/row_cursor.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"

namespace doris {

MemTable::MemTable(Schema* schema, const TabletSchema* tablet_schema,
                   std::vector<uint32_t>* col_ids, TupleDescriptor* tuple_desc,
                   KeysType keys_type)
    : _schema(schema),
      _tablet_schema(tablet_schema),
      _tuple_desc(tuple_desc),
      _col_ids(col_ids),
      _keys_type(keys_type),
      _row_comparator(_schema) {
    _schema_size = _schema->schema_size();
    _tuple_buf = _arena.Allocate(_schema_size);
    _skip_list = new Table(_row_comparator, &_arena);
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema)
    : _schema(schema) {}

int MemTable::RowCursorComparator::operator()
    (const char* left, const char* right) const {
    return _schema->compare(left, right);
}

size_t MemTable::memory_usage() {
    return _arena.MemoryUsage();
}

void MemTable::insert(Tuple* tuple) {
    const std::vector<SlotDescriptor*>& slots = _tuple_desc->slots();
    size_t offset = 0;
    for (size_t i = 0; i < _col_ids->size(); ++i) {
        const SlotDescriptor* slot = slots[(*_col_ids)[i]];
        _schema->set_not_null(i, _tuple_buf);
        if (tuple->is_null(slot->null_indicator_offset())) {
            _schema->set_null(i, _tuple_buf);
            offset += _schema->get_col_size(i) + 1;
            continue;
        }
        offset += 1;
        void* value = tuple->get_slot(slot->tuple_offset());
        _schema->ingest_raw_value(i, _tuple_buf + offset, value, &_arena);
        offset = offset + _schema->get_col_size(i);
    }

    bool overwritten = false;
    _skip_list->Insert(_tuple_buf, &overwritten, _keys_type);
    if (!overwritten) {
        _tuple_buf = _arena.Allocate(_schema_size);
    }
}

OLAPStatus MemTable::flush(RowsetWriterSharedPtr rowset_writer) {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Table::Iterator it(_skip_list);
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            const char* row = it.key();
            _schema->finalize(row, &_arena);
            RETURN_NOT_OK(rowset_writer->add_row(row, _schema));
        }
        RETURN_NOT_OK(rowset_writer->flush());
    }
    DorisMetrics::memtable_flush_total.increment(1); 
    DorisMetrics::memtable_flush_duration_us.increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close(RowsetWriterSharedPtr rowset_writer) {
    return flush(rowset_writer);
}

} // namespace doris
