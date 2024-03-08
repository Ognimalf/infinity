// Copyright(C) 2023 InfiniFlow, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "unit_test/base_test.h"

import infinity_context;
import infinity_exception;
import infinity_mock;

import stl;
import global_resource_usage;
import third_party;
import logger;
import table_def;
import column_def;
import data_type;
import logical_type;
import value;

import data_block;
import default_values;
import txn_manager;
import txn;
import catalog;
import status;
import extra_ddl_info;

import base_entry;

using namespace infinity;

class CatalogTest : public BaseTest {
    void SetUp() override {
        BaseTest::SetUp();
        system("rm -rf /tmp/infinity/log /tmp/infinity/data /tmp/infinity/wal");
        infinity::GlobalResourceUsage::Init();
        std::shared_ptr<std::string> config_path = nullptr;
    }

    void TearDown() override {
        EXPECT_EQ(infinity::GlobalResourceUsage::GetObjectCount(), 0);
        EXPECT_EQ(infinity::GlobalResourceUsage::GetRawMemoryCount(), 0);
        infinity::GlobalResourceUsage::UnInit();
        BaseTest::TearDown();
    }
};

UniquePtr<TableDef> MockTestTableDesc() {
    // Define columns
    Vector<SharedPtr<ColumnDef>> columns;
    {
        i64 column_id = 0;
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kUnique);
            constraints.insert(ConstraintType::kNotNull);
            auto column_def_ptr =
                MakeShared<ColumnDef>(column_id++, MakeShared<DataType>(DataType(LogicalType::kTinyInt)), "tiny_int_col", constraints);
            columns.emplace_back(column_def_ptr);
        }
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kPrimaryKey);
            auto column_def_ptr =
                MakeShared<ColumnDef>(column_id++, MakeShared<DataType>(DataType(LogicalType::kBigInt)), "big_int_col", constraints);
            columns.emplace_back(column_def_ptr);
        }
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kNotNull);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++, MakeShared<DataType>(DataType(LogicalType::kDouble)), "double_col", constraints);
            columns.emplace_back(column_def_ptr);
        }
    }

    UniquePtr<TableDef> tbl1_def = MakeUnique<TableDef>(MakeShared<String>("default"), MakeShared<String>("tbl1"), columns);
    return MakeUnique<TableDef>(MakeShared<String>("crash"), MakeShared<String>("tbl1"), columns);
}

SharedPtr<DataBlock> MockDataBlock() {
    SharedPtr<DataBlock> data_block = MakeShared<DataBlock>();

    Vector<SharedPtr<DataType>> column_types;
    column_types.emplace_back(MakeShared<DataType>(LogicalType::kTinyInt));
    column_types.emplace_back(MakeShared<DataType>(LogicalType::kBigInt));
    column_types.emplace_back(MakeShared<DataType>(LogicalType::kDouble));

    SizeT row_count = DEFAULT_VECTOR_SIZE;
    data_block->Init(column_types, row_count);

    for (SizeT i = 0; i < row_count; ++i) {
        data_block->AppendValue(0, Value::MakeTinyInt(static_cast<i8>(i)));
    }

    for (SizeT i = 0; i < row_count; ++i) {
        data_block->AppendValue(1, Value::MakeBigInt(static_cast<i64>(i)));
    }

    for (SizeT i = 0; i < row_count; ++i) {
        data_block->AppendValue(2, Value::MakeDouble(static_cast<f64>(i)));
    }

    data_block->Finalize();

    return data_block;
}

// txn1: create db1, get db1, delete db1, get db1, commit
// txn2:             get db1,             get db1, commit
TEST_F(CatalogTest, simple_test1) {
    using namespace infinity;
    infinity::InfinityContext::instance().Init(config_path);

    TxnManager *txn_mgr = infinity::InfinityContext::instance().storage()->txn_manager();
    Catalog *catalog = infinity::InfinityContext::instance().storage()->catalog();

    // start txn1
    auto *txn1 = txn_mgr->CreateTxn();
    txn1->Begin();

    // start txn2
    auto *txn2 = txn_mgr->CreateTxn();
    txn2->Begin();
    HashMap<String, BaseEntry *> databases;

    // create db in empty catalog should be success
    {
        auto [base_entry, status] = catalog->CreateDatabase("db1", txn1->TxnID(), txn1->BeginTS(), txn_mgr);
        EXPECT_TRUE(status.ok());
        // store this entry
        databases["db1"] = base_entry;
    }

    {
        auto [db_entry1, status1] = catalog->GetDatabase("db1", txn1->TxnID(), txn1->BeginTS());
        // should be visible to same txn
        EXPECT_TRUE(status1.ok());
        EXPECT_EQ(db_entry1, databases["db1"]);

        // should not be visible to other txn
        auto [db_entry2, status2] = catalog->GetDatabase("db1", txn2->TxnID(), txn2->BeginTS());
        EXPECT_TRUE(!status2.ok());
        EXPECT_EQ(db_entry2, nullptr);
    }

    // drop db should be success
    {
        auto [base_entry, status] = catalog->DropDatabase("db1", txn1->TxnID(), txn1->BeginTS(), txn_mgr);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(base_entry, databases["db1"]);
        // remove this entry
        databases.erase("db1");

        auto [db_entry1, status1] = catalog->GetDatabase("db1", txn1->TxnID(), txn1->BeginTS());
        // should not be visible to same txn
        EXPECT_TRUE(!status1.ok());

        // should not be visible to other txn
        auto [db_entry2, status2] = catalog->GetDatabase("db1", txn2->TxnID(), txn2->BeginTS());
        EXPECT_TRUE(!status2.ok());
    }

    txn_mgr->CommitTxn(txn1);
    txn_mgr->CommitTxn(txn2);
    infinity::InfinityContext::instance().UnInit();
}

// txn1: create db1, commit.
// txn2: start,              get db1, commit
// txn3:                     start, get db1, delete db1, commit
TEST_F(CatalogTest, simple_test2) {
    using namespace infinity;
    infinity::InfinityContext::instance().Init(config_path);

    TxnManager *txn_mgr = infinity::InfinityContext::instance().storage()->txn_manager();
    Catalog *catalog = infinity::InfinityContext::instance().storage()->catalog();

    // start txn1
    auto *txn1 = txn_mgr->CreateTxn();
    txn1->Begin();

    // start txn2
    auto *txn2 = txn_mgr->CreateTxn();
    txn2->Begin();

    // create db in empty catalog should be success
    {
        Status status = txn1->CreateDatabase("db1", ConflictType::kError);
        EXPECT_TRUE(status.ok());
    }

    u64 txn1_id = txn1->TxnID();
    TxnTimeStamp txn1_begin_ts = txn1->BeginTS();
    txn_mgr->CommitTxn(txn1);

    // should not be visible to txn2
    {
        auto [db_entry, status1] = catalog->GetDatabase("db1", txn1_id, txn1_begin_ts);
        // should not be visible to same txn
        EXPECT_TRUE(!status1.ok());
    }

    txn_mgr->CommitTxn(txn2);

    auto *txn3 = txn_mgr->CreateTxn();
    txn3->Begin();

    // should be visible to txn3
    {
        auto [db_entry1, status1] = catalog->GetDatabase("db1", txn3->TxnID(), txn3->BeginTS());
        EXPECT_TRUE(status1.ok());
        EXPECT_NE(db_entry1, nullptr);

        Status status = txn3->DropDatabase("db1", ConflictType::kError);
        EXPECT_TRUE(status.ok());

        // should not be visible to other txn
        auto [db_entry2, status2] = catalog->Catalog::GetDatabase("db1", txn3->TxnID(), txn3->BeginTS());
        EXPECT_TRUE(!status2.ok());
    }

    txn_mgr->CommitTxn(txn3);
    infinity::InfinityContext::instance().UnInit();
}

TEST_F(CatalogTest, concurrent_test) {
    using namespace infinity;
    infinity::InfinityContext::instance().Init(config_path);

    TxnManager *txn_mgr = infinity::InfinityContext::instance().storage()->txn_manager();
    Catalog *catalog = infinity::InfinityContext::instance().storage()->catalog();

    for (int loop = 0; loop < 1; ++loop) {
        // start txn1 && txn2
        auto *txn1 = txn_mgr->CreateTxn();
        txn1->Begin();
        auto *txn2 = txn_mgr->CreateTxn();
        txn2->Begin();

        auto write_routine = [&](int start, Txn *txn) {
            for (int db_id = start; db_id < 1000; db_id += 2) {
                String db_name = "db" + std::to_string(db_id);
                Status status = txn->CreateDatabase(db_name, ConflictType::kError);
                EXPECT_TRUE(status.ok());
            }
        };

        Thread write_thread1(write_routine, 0, txn1);
        Thread write_thread2(write_routine, 1, txn2);

        write_thread1.join();
        write_thread2.join();

        txn_mgr->CommitTxn(txn1);
        txn_mgr->CommitTxn(txn2);

        // start txn3 && txn4
        auto *txn3 = txn_mgr->CreateTxn();
        txn3->Begin();
        auto *txn4 = txn_mgr->CreateTxn();
        txn4->Begin();

        auto read_routine = [&](Txn *txn) {
            for (int db_id = 0; db_id < 1000; ++db_id) {
                String db_name = "db" + std::to_string(db_id);
                auto [db_entry, status] = catalog->Catalog::GetDatabase(db_name, txn->TxnID(), txn->BeginTS());
                EXPECT_TRUE(status.ok());
                // only read, don't need lock
                EXPECT_NE(db_entry, nullptr);
            }
        };

        std::thread read_thread1(read_routine, txn3);
        std::thread read_thread2(read_routine, txn4);
        read_thread1.join();
        read_thread2.join();

        txn_mgr->CommitTxn(txn3);
        txn_mgr->CommitTxn(txn4);

        // start txn5 && txn6
        auto *txn5 = txn_mgr->CreateTxn();
        txn5->Begin();
        auto *txn6 = txn_mgr->CreateTxn();
        txn6->Begin();

        auto drop_routine = [&](int start, Txn *txn) {
            for (int db_id = start; db_id < 1000; db_id += 2) {
                String db_name = "db" + std::to_string(db_id);
                Status status = txn->DropDatabase(db_name, ConflictType::kError);
                EXPECT_TRUE(status.ok());
            }
        };

        std::thread drop_thread1(drop_routine, 0, txn5);
        std::thread drop_thread2(drop_routine, 1, txn6);
        drop_thread1.join();
        drop_thread2.join();

        txn_mgr->CommitTxn(txn5);
        txn_mgr->CommitTxn(txn6);

        // start txn7
        auto *txn7 = txn_mgr->CreateTxn();
        txn7->Begin();

        // check all has been dropped
        for (int db_id = 0; db_id < 1000; ++db_id) {
            String db_name = "db" + std::to_string(db_id);
            auto [db_entry, status] = catalog->Catalog::GetDatabase(db_name, txn7->TxnID(), txn7->BeginTS());
            EXPECT_TRUE(!status.ok());
        }
    }
    infinity::InfinityContext::instance().UnInit();
}

TEST_F(CatalogTest, crash_test_block_flush) {
    if (true) return;
    using namespace infinity;
    std::shared_ptr<std::string> config_path = nullptr;
    MockInfinityContext::instance().Init(config_path, false, false, true, false);

    TxnManager *txn_mgr = MockInfinityContext::instance().storage()->txn_manager();

    // initialize catalog
    {
        auto *txn = txn_mgr->CreateTxn();
        txn->Begin();
        auto [database_entry, db_status] = txn->CreateDatabase("crash", infinity::ConflictType::kIgnore);
        auto [table_entry, table_status] = txn->CreateTable("crash", MockTestTableDesc(), infinity::ConflictType::kIgnore);
        txn_mgr->CommitTxn(txn);
    }

    // write block
    {
        auto *txn = txn_mgr->CreateTxn();
        auto data_block = MockDataBlock();

        txn->Begin();
        txn->Append("crash", "tbl1", data_block);

        txn_mgr->CommitTxn(txn);
        MockInfinityContext::instance().UnInit();
    }

    // restart
    {
        MockInfinityContext::instance().Init(config_path, false, false, true, false);
        auto *txn = txn_mgr->CreateTxn();
        txn->Begin();

        auto [db_entry, db_status] = txn->GetDatabase("crash");
        EXPECT_TRUE(db_status.ok());
        EXPECT_NE(db_entry, nullptr);

        auto [table_entry, table_status] = txn->GetTableEntry("crash", "tbl1");
        EXPECT_TRUE(table_status.ok());
        EXPECT_NE(table_entry, nullptr);

        auto segment_entry = table_entry->GetSegmentByID(0, txn->BeginTS());
        EXPECT_EQ(segment_entry, nullptr);

        txn_mgr->CommitTxn(txn);
    }
    MockInfinityContext::instance().UnInit();
}

// TEST_F(CatalogTest, crash_test_catalog_full_flush) {
//  TODO:
// }

// TEST_F(CatalogTest, crash_test_gc_wal) {
//  TODO:
// }
