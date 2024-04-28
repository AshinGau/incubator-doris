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

#include "exprs/create_predicate_function.h"
#include "util/runtime_profile.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class EqualityDeleteBase {
protected:
    RuntimeProfile::Counter* num_delete_rows;
    RuntimeProfile::Counter* build_set_time;
    RuntimeProfile::Counter* equality_delete_time;

    Block* _delete_block;

    virtual Status _build_set() = 0;

public:
    EqualityDeleteBase(Block* delete_block) : _delete_block(delete_block) {}
    virtual ~EqualityDeleteBase() = default;

    Status init(RuntimeProfile* profile) {
        static const char* delete_profile = "EqualityDelete";
        ADD_TIMER_WITH_LEVEL(profile, delete_profile, 1);
        num_delete_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumRowsInDeleteFile", TUnit::UNIT,
                                                       delete_profile, 1);
        build_set_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "BuildHashSetTime", delete_profile, 1);
        equality_delete_time =
                ADD_CHILD_TIMER_WITH_LEVEL(profile, "EqualityDeleteFilterTime", delete_profile, 1);
        SCOPED_TIMER(build_set_time);
        return _build_set();
    }

    virtual Status filter_data_block(Block* data_block) = 0;

    static std::unique_ptr<EqualityDeleteBase> get_delete_impl(Block* delete_block);
};

class SimpleEqualityDelete : public EqualityDeleteBase {
protected:
    std::shared_ptr<HybridSetBase> _hybrid_set;
    std::string _delete_column_name;
    std::unique_ptr<IColumn::Filter> _filter;

    Status _build_set() override;

public:
    SimpleEqualityDelete(Block* delete_block) : EqualityDeleteBase(delete_block) {}

    Status filter_data_block(Block* data_block) override;
};

class MultiEqualityDelete : public EqualityDeleteBase {
protected:
    std::vector<uint64_t> _delete_hashes;
    std::vector<uint64_t> _data_hashes;
    // hash code => row index
    flat_hash_map<uint64_t, size_t> _delete_hash_map;
    std::vector<size_t> _data_column_index;
    std::unique_ptr<IColumn::Filter> _filter;

    Status _build_set() override;

    bool _equal(Block* data_block, size_t data_row_index, size_t delete_row_index);

public:
    MultiEqualityDelete(Block* delete_block) : EqualityDeleteBase(delete_block) {}

    Status filter_data_block(Block* data_block) override;
};

} // namespace doris::vectorized