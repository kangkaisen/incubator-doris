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

#include <algorithm>
#include <map>
#include <sstream>
#include <string>
#include "olap/field.h"
#include "exprs/aggregate_functions.h"

using std::map;
using std::nothrow;
using std::string;

namespace doris {

Field::Field(const TabletColumn& column)
        : _type(column.type()),
          _index_size(column.index_length()),
          _offset(0) {
    _type_info = get_type_info(column.type());
    _size = _type_info->size();
    _length = column.length();
    _index_size = column.index_length();
    _agg_info = get_aggregate_info(column.aggregation(), column.type());
}

}  // namespace doris
