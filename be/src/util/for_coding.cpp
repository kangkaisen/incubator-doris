//
// Created by kangkaisen on 2019-09-10.
//

#include "for_coding.h"

#include <iostream>

namespace doris {
    static inline uint32_t bits(const uint32_t v) {
        return v == 0 ? 0 : 32 - __builtin_clz(v);
    }

    void ForEncoder::put(int32_t value) {
        _buffered_values[_buffered_values_num++] = value;
        if (_buffered_values_num == _frame_value_num) {
            flush_buffered_valued();
        }
    }

    void ForEncoder::flush_buffered_valued() {
        auto *out = reinterpret_cast<uint32_t*>(_buffer->data());
        uint32_t min = _buffered_values[0];
        uint32_t max = _buffered_values[0];
        for(uint8_t i = 1; i < _buffered_values_num; ++i) {
            if(_buffered_values[i] < min) {
                min = _buffered_values[i];
            }
            if(_buffered_values[i] < max) {
                max = _buffered_values[i];
            }
        }
        out[0] = min;
        std::cout << "min is" << min;
        std::cout << "out[0]  is" <<  out[0];
        out++;

        out[0] = max;
        std::cout << "max is" << max;
        std::cout << "out[0]  is" <<  out[0];
        out++;

        int bit_width = bits(static_cast<uint32_t>(max - min));
        std::cout << "bit_width is" << bit_width;

        uint32_t* in = &_buffered_values[0];
        for(uint8_t k = 0; k + 32 <= _buffered_values_num; k += 32, in += 32) {
            out = pack32[bit_width](min, in, out);
        }

        _buffered_values_num = 0;
    }

    uint32_t ForEncoder::flush() {
        if (_buffered_values_num != 0) {
            flush_buffered_valued();
        }

        return _buffer->length();
    }

    uint32_t ForEncoder::len() {
        return _buffer->length();
    }


    void ForDecoder::decode(uint32_t *out) {
        uint8_t value_count = _frame_value_num;
        uint32_t min = _in[0];
        _in++;
        std::cout << "min is" << min;

        uint32_t max = _in[0];
        _in++;
        std::cout << "max is" << max;

        int bit_width = bits(static_cast<uint32_t>(max - min));

        for(uint32_t k = 0; k< value_count / 32; ++k) {
            unpack32[bit_width](min, _in + bit_width * k, out + 32 * k);
        }
    }

}
