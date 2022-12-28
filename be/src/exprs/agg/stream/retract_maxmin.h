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

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/stream/stream_detail_state.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "util/raw_container.h"

namespace starrocks {

template <LogicalType PT, typename = guard::Guard>
struct MaxAggregateDataRetractable : public StreamDetailState<PT> {};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, FixedLengthPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;

    MaxAggregateDataRetractable() {}

    T result = RunTimeTypeLimits<PT>::min_value();
    void reset_result() { result = RunTimeTypeLimits<PT>::min_value(); }

    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT>
struct MaxAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset_result() {
        buffer.clear();
        size = -1;
    }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT, typename = guard::Guard>
struct MinAggregateDataRetractable : public StreamDetailState<PT> {};
template <LogicalType PT>
struct MinAggregateDataRetractable<PT, FixedLengthPTGuard<PT>> : public StreamDetailState<PT> {
    using T = RunTimeCppType<PT>;

    T result = RunTimeTypeLimits<PT>::max_value();
    void reset_result() { result = RunTimeTypeLimits<PT>::max_value(); }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT>
struct MinAggregateDataRetractable<PT, StringPTGuard<PT>> : public StreamDetailState<PT> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset_result() {
        buffer.clear();
        size = -1;
    }
    void reset() {
        StreamDetailState<PT>::reset();
        reset_result();
    }
};

template <LogicalType PT, typename State, class OP, typename T = RunTimeCppType<PT>>
class MaxMinAggregateFunctionRetractable final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunctionRetractable<PT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        T value = get_row_value(column, row_num);
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if constexpr (pt_is_string<PT>) {
            DCHECK(to->is_binary());
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(this->data(state).slice());
        } else {
            DCHECK(!to->is_nullable() && !to->is_binary());
            down_cast<InputColumnType*>(to)->append(this->data(state).result);
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if constexpr (pt_is_string<PT>) {
            DCHECK(to->is_binary());
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(this->data(state).slice());
        } else {
            DCHECK(!to->is_nullable() && !to->is_binary());
            down_cast<InputColumnType*>(to)->append(this->data(state).result);
        }
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        if constexpr (pt_is_string<PT>) {
            auto* column = down_cast<BinaryColumn*>(dst);
            for (size_t i = start; i < end; ++i) {
                column->append(this->data(state).slice());
            }
        } else {
            InputColumnType* column = down_cast<InputColumnType*>(dst);
            for (size_t i = start; i < end; ++i) {
                column->get_data()[i] = this->data(state).result;
            }
        }
    }

    // MV METHODS

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override {
        if (is_append_only) {
            return AggStateTableKind::RESULT;
        } else {
            return AggStateTableKind::DETAIL_RESULT;
        }
    }

    T get_row_value(const Column* column, size_t row_num) const {
        if constexpr (pt_is_string<PT>) {
            DCHECK(column->is_binary());
            return column->get(row_num).get_slice();
        } else {
            DCHECK(!column->is_nullable() && !column->is_binary());
            const auto& col = down_cast<const InputColumnType&>(*column);
            return col.get_data()[row_num];
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        T value = get_row_value(columns[0], row_num);
        this->data(state).update_rows(value, 1);
        // If is_sync, use detail state in the final.
        if (!this->data(state).is_sync()) {
            OP()(this->data(state), value);
        }
    }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        T value = get_row_value(columns[0], row_num);
        this->data(state).update_rows(value, -1);

        // reset state to restore from detail
        if (!this->data(state).is_sync() && OP::is_sync(this->data(state), value)) {
            this->data(state).set_is_sync(true);
            this->data(state).reset_result();
        }
    }

    void restore_detail(FunctionContext* ctx, size_t num_row, const std::vector<const Column*>& columns,
                        AggDataPtr __restrict state) const override {
        T value = get_row_value(columns[0], num_row);
        DCHECK((*columns[1]).is_numeric());
        int64_t count = columns[1]->get(0).get_int64();
        // If the incremenatal already had the data, ignore.
        (this->data(state)).template update_rows<true>(value, count);
    }

    void restore_all_details(FunctionContext* ctx, AggDataPtr __restrict state, size_t chunk_size,
                             const Columns& columns) const override {
        DCHECK(this->data(state).is_sync());

        // sync incremental data
        if (!this->data(state).is_restore_incremental()) {
            auto& detail_state = this->data(state).detail_state();
            for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
                if (iter->second <= 0) {
                    continue;
                }
                const T& value = iter->first;
                OP()(this->data(state), value);
            }
            this->data(state).set_is_restore_incremental(true);
        }

        // sync previous records from detail state table
        InputColumnType* column;
        if constexpr (!pt_is_string<PT>) {
            column = down_cast<InputColumnType*>(columns[0].get());
        }
        DCHECK((*columns[1]).is_numeric());
        for (size_t i = 0; i < chunk_size; i++) {
            T value = get_row_value(columns[0].get(), i);
            // If the incremenatal already had the data, ignore.
            if (this->data(state).exists(value)) {
                continue;
            }
            OP()(this->data(state), value);
        }
    }

    void output_is_sync(FunctionContext* ctx, size_t chunk_size, Column* to,
                        AggDataPtr __restrict state) const override {
        UInt8Column* sync_col = down_cast<UInt8Column*>(to);
        uint8_t is_sync = this->data(state).is_sync();
        sync_col->append(is_sync);
    }

    void output_detail(FunctionContext* ctx, ConstAggDataPtr __restrict state, const Columns& to,
                       Column* count) const override {
        if constexpr (pt_is_string<PT>) {
            DCHECK((*to[0]).is_binary());
        } else {
            DCHECK((*to[0]).is_numeric());
        }
        DCHECK((*to[1]).is_numeric());
        InputColumnType* column0 = down_cast<InputColumnType*>(to[0].get());
        Int64Column* column1 = down_cast<Int64Column*>(to[1].get());
        auto& detail_state = this->data(state).detail_state();
        for (auto iter = detail_state.cbegin(); iter != detail_state.cend(); iter++) {
            // is it possible that count is negative?
            DCHECK_LE(0, iter->second);
            column0->append(iter->first);
            column1->append(iter->second);
        }
        Int64Column* count_col = down_cast<Int64Column*>(count);
        count_col->append(detail_state.size());
    }
    std::string get_name() const override { return "retract_maxmin"; }
};

} // namespace starrocks