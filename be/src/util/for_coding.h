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

#ifndef DORIS_FOR_CODING_H
#define DORIS_FOR_CODING_H


#include <cstdlib>

#include "util/bpacking.h"
#include "util/faststring.h"

namespace doris {
class ForEncoder {
public:
    explicit ForEncoder(faststring *buffer): _buffer(buffer) {
    }

    void put(int32_t value);

    void flush_buffered_valued();
    // Flushes any pending values to the underlying buffer.
    // Returns the total number of bytes written
    uint32_t flush();

    uint32_t len();

    // Resets all the state in the encoder.
    void clear();
private:
    uint8_t count = 0;
    uint8_t _frame_value_num = 128;
    uint8_t _buffered_values_num = 0;
    uint32_t _buffered_values[128];

    // Underlying buffer.
    faststring* _buffer;
};

class ForDecoder {
public:
    explicit ForDecoder(uint32_t* buffer): _in(buffer){
    };
    void decode (uint32_t * out);

    bool Get(int32_t* val);

    // Skip n values, and returns the number of non-zero entries skipped.
    size_t Skip(size_t to_skip);

private:
    uint8_t _frame_value_num = 128;
    uint32_t* _in;
};
}


#endif //DORIS_FOR_CODING_H
