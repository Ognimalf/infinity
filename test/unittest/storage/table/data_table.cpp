//
// Created by jinhai on 23-7-30.
//

#include <gtest/gtest.h>
#include "base_test.h"
#include "storage/table_def.h"
#include "storage/table/data_table.h"
#include "storage/txn/txn_manager.h"
#include "main/profiler/base_profiler.h"
#include "main/logger.h"
#include "main/stats/global_resource_usage.h"
#include "main/infinity.h"

class DataTableTest : public BaseTest {
    void
    SetUp() override {
        infinity::GlobalResourceUsage::Init();
        std::shared_ptr<std::string> config_path = nullptr;
        infinity::Infinity::instance().Init(config_path);

        system("rm -rf /tmp/infinity/data/table");
        system("rm -rf /tmp/infinity/_tmp");
    }

    void
    TearDown() override {
        infinity::Infinity::instance().UnInit();
        EXPECT_EQ(infinity::GlobalResourceUsage::GetObjectCount(), 0);
        EXPECT_EQ(infinity::GlobalResourceUsage::GetRawMemoryCount(), 0);
        infinity::GlobalResourceUsage::UnInit();
    }
};


TEST_F(DataTableTest, test1) {
#if 0

    using namespace infinity;

    SizeT memory_limit = 1024 * 1024 * 1024; // 1 Gib
    SharedPtr<String> temp_path = MakeShared<String>("/tmp/infinity/_tmp");
    SharedPtr<String> base_path = MakeShared<String>("/tmp/infinity/data");
    BufferManager buffer_mgr(memory_limit, base_path, temp_path);

    SharedPtr<String> table_dir = MakeShared<String>("/tmp/infinity/table");
    SharedPtr<TableDef> table_def{};

    // Define columns
    {
        Vector<SharedPtr<ColumnDef>> columns;
        i64 column_id = 0;
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kUnique);
            constraints.insert(ConstraintType::kNotNull);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++,
                                                        MakeShared<DataType>(DataType(LogicalType::kTinyInt)),
                                                        "tiny_int_col",
                                                        constraints);
            columns.emplace_back(column_def_ptr);

        }
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kPrimaryKey);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++,
                                                        MakeShared<DataType>(DataType(LogicalType::kBigInt)),
                                                        "big_int_col",
                                                        constraints);
            columns.emplace_back(column_def_ptr);
        }

        table_def = TableDef::Make(MakeShared<String>("default"), MakeShared<String>("t1"), columns);

        EXPECT_EQ(table_def->table_name(), "t1");
        EXPECT_EQ(table_def->column_count(), 2);
        EXPECT_EQ(table_def->GetColIdByName("tiny_int_col"), 0);
        EXPECT_EQ(table_def->GetColIdByName("big_int_col"), 1);
        LOG_TRACE("\n{}", table_def->ToString());
    }

    SharedPtr<TableEntry> table_entry = MakeShared<TableEntry>(table_dir, table_def, nullptr, 0, 0, nullptr);

    SharedPtr<DataTable> data_table = DataTable::Make(table_dir, table_def, &buffer_mgr);
    EXPECT_EQ(data_table->table_name(), "t1");
    EXPECT_EQ(data_table->schema_name(), "default");
}

TEST_F(DataTableTest, test2) {
    using namespace infinity;

    SizeT memory_limit = 1024 * 1024 * 1024; // 1 Gib
    SharedPtr<String> temp_path = MakeShared<String>("/tmp/infinity/_tmp");
    SharedPtr<String> base_path = MakeShared<String>("/tmp/infinity/data");
    BufferManager buffer_mgr(memory_limit, base_path, temp_path);

//    UniquePtr<String> dir = MakeUnique<String>("/tmp/infinity/table");
    UniquePtr<String> dir = MakeUnique<String>("table");
    NewCatalog new_catalog(std::move(dir), &buffer_mgr, nullptr);
    TxnManager txn_mgr(&new_catalog);

    EntryResult create1_res, table1_res, get_res;

    // Txn1: Create, OK
    Txn* new_txn = txn_mgr.CreateTxn();

    // Txn1: Begin, OK
    new_txn->BeginTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    // Txn1: Create db1, OK
    create1_res = new_txn->CreateDatabase("db1");
    EXPECT_EQ(create1_res.entry_->Committed(), false);
    EXPECT_NE(create1_res.entry_, nullptr);

    // Txn1: Create tbl1, OK
    // Define columns
    Vector<SharedPtr<ColumnDef>> columns;
    {
        i64 column_id = 0;
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kUnique);
            constraints.insert(ConstraintType::kNotNull);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++,
                                                        MakeShared<DataType>(DataType(LogicalType::kTinyInt)),
                                                        "tiny_int_col",
                                                        constraints);
            columns.emplace_back(column_def_ptr);

        }
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kPrimaryKey);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++,
                                                        MakeShared<DataType>(DataType(LogicalType::kBigInt)),
                                                        "big_int_col",
                                                        constraints);
            columns.emplace_back(column_def_ptr);
        }
        {
            HashSet<ConstraintType> constraints;
            constraints.insert(ConstraintType::kNotNull);
            auto column_def_ptr = MakeShared<ColumnDef>(column_id++,
                                                        MakeShared<DataType>(DataType(LogicalType::kDouble)),
                                                        "double_col",
                                                        constraints);
            columns.emplace_back(column_def_ptr);
        }
    }

    UniquePtr<TableDef> tbl1_def = MakeUnique<TableDef>("tbl1", columns);
    table1_res = new_txn->CreateTable("db1", std::move(tbl1_def));
    EXPECT_NE(table1_res.entry_, nullptr);

    // Txn1: Commit, OK
    new_txn->CommitTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    {
        // Txn2: Create, OK
        new_txn = txn_mgr.CreateTxn();

        // Txn2: Begin, OK
        new_txn->BeginTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());

        // Txn2: Get db1, OK
        get_res = new_txn->GetTableByName("db1", "tbl1");
        EXPECT_NE(get_res.entry_, nullptr);

        // Prepare the input data block
        Vector<SharedPtr<DataType>> column_types;
        column_types.emplace_back(MakeShared<DataType>(LogicalType::kTinyInt));
        column_types.emplace_back(MakeShared<DataType>(LogicalType::kBigInt));
        column_types.emplace_back(MakeShared<DataType>(LogicalType::kDouble));

        SharedPtr<DataBlock> input_block = MakeShared<DataBlock>();

        SizeT row_count = DEFAULT_VECTOR_SIZE * 2;
        input_block->Init(column_types, row_count);

        for (SizeT i = 0; i < row_count; ++i) {
            input_block->AppendValue(0, Value::MakeTinyInt(static_cast<i8>(i)));
        }

        for (SizeT i = 0; i < row_count; ++i) {
            input_block->AppendValue(1, Value::MakeBigInt(static_cast<i64>(i)));
        }

        for (SizeT i = 0; i < row_count; ++i) {
            input_block->AppendValue(2, Value::MakeDouble(static_cast<f64>(i)));
        }

        input_block->Finalize();
        EXPECT_EQ(input_block->Finalized(), true);

        new_txn->Append("db1", "tbl1", input_block);

        // Txn2: Commit, OK
        new_txn->CommitTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    }

    {
        // Txn2: Create, OK
        new_txn = txn_mgr.CreateTxn();

        // Txn2: Begin, OK
        new_txn->BeginTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());

        {
            // Get column 0 and column 2 from global storage;
            Vector<ColumnID> column_ids{0, 2};

            UniquePtr<MetaTableState> read_table_meta = new_txn->GetTableMeta("db1", "tbl1", column_ids);
            EXPECT_EQ(read_table_meta->local_blocks_.size(), 0);
            EXPECT_EQ(read_table_meta->segment_map_.size(), 1);
            for (const auto &segment_pair: read_table_meta->segment_map_) {
                EXPECT_EQ(segment_pair.first, 0);
                EXPECT_NE(segment_pair.second.data_segment_, nullptr);
                EXPECT_EQ(segment_pair.second.column_data_map_.size(), 2);
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(0));
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(2));
                ColumnData *column0 = segment_pair.second.column_data_map_.at(0).column_data_;
                ColumnData *column2 = segment_pair.second.column_data_map_.at(2).column_data_;

                SizeT row_count = segment_pair.second.data_segment_->RowCount();
                ObjectHandle col0_obj = column0->GetColumnData();
                i8 *col0_ptr = (i8 *) (col0_obj.GetData());
                for (SizeT row = 0; row < row_count; ++row) {
//                LOG_TRACE("COL0 ROW: {}, value: {}", row, (i16)(col0_ptr[row]));
                    EXPECT_EQ(col0_ptr[row], (i8) (row));
                }

                ObjectHandle col2_obj = column2->GetColumnData();
                f64 *col2_ptr = (f64 *) (col2_obj.GetData());
                for (SizeT row = 0; row < row_count; ++row) {
                    EXPECT_FLOAT_EQ(col2_ptr[row], row % 8192);
                }
            }
        }

        // Append more data into local storage
        {
            // Prepare the input data block
            Vector<SharedPtr<DataType>> column_types;
            column_types.emplace_back(MakeShared<DataType>(LogicalType::kTinyInt));
            column_types.emplace_back(MakeShared<DataType>(LogicalType::kBigInt));
            column_types.emplace_back(MakeShared<DataType>(LogicalType::kDouble));

            SharedPtr<DataBlock> input_block = MakeShared<DataBlock>();

            SizeT row_count = DEFAULT_VECTOR_SIZE;
            input_block->Init(column_types, row_count);

            for (SizeT i = 0; i < row_count; ++i) {
                input_block->AppendValue(0, Value::MakeTinyInt(static_cast<i8>(i)));
            }

            for (SizeT i = 0; i < row_count; ++i) {
                input_block->AppendValue(1, Value::MakeBigInt(static_cast<i64>(i)));
            }

            for (SizeT i = 0; i < row_count; ++i) {
                input_block->AppendValue(2, Value::MakeDouble(static_cast<f64>(i)));
            }

            input_block->Finalize();
            EXPECT_EQ(input_block->Finalized(), true);

            new_txn->Append("db1", "tbl1", input_block);
        }

        {
            // Get column 0 and column 2 from local and global storage;
            Vector<ColumnID> column_ids{0, 2};

            UniquePtr<MetaTableState> read_table_meta = new_txn->GetTableMeta("db1", "tbl1", column_ids);
            EXPECT_EQ(read_table_meta->local_blocks_.size(), 1);
            for(const auto& local_block_state: read_table_meta->local_blocks_) {
                EXPECT_NE(local_block_state.data_block_, nullptr);
                SizeT row_count = local_block_state.data_block_->row_count();
                EXPECT_EQ(row_count, 8192);
                EXPECT_EQ(local_block_state.column_vector_map_.size(), 2);

                ColumnVector* column0 = local_block_state.column_vector_map_.at(0).column_vector_;
                i8* col0_ptr = (i8*)(column0->data_ptr_);
                ColumnVector* column2 = local_block_state.column_vector_map_.at(2).column_vector_;
                f64* col2_ptr = (f64*)(column2->data_ptr_);
                for(SizeT row = 0; row < row_count; ++ row) {
                    EXPECT_EQ(col0_ptr[row], (i8)row);
                    EXPECT_FLOAT_EQ(col2_ptr[row], row % 8192);
                }
            }

            EXPECT_EQ(read_table_meta->segment_map_.size(), 1);
            for(const auto& segment_pair: read_table_meta->segment_map_) {
                EXPECT_EQ(segment_pair.first, 0);
                EXPECT_NE(segment_pair.second.data_segment_, nullptr);
                EXPECT_EQ(segment_pair.second.column_data_map_.size(), 2);
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(0));
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(2));
                ColumnData* column0 = segment_pair.second.column_data_map_.at(0).column_data_;
                ColumnData* column2 = segment_pair.second.column_data_map_.at(2).column_data_;

                SizeT row_count = segment_pair.second.data_segment_->RowCount();
                ObjectHandle col0_obj = column0->GetColumnData();
                i8* col0_ptr = (i8*)(col0_obj.GetData());
                for(SizeT row = 0; row < row_count; ++ row) {
//                LOG_TRACE("COL0 ROW: {}, value: {}", row, (i16)(col0_ptr[row]));
                    EXPECT_EQ(col0_ptr[row], (i8)(row));
                }

                ObjectHandle col2_obj = column2->GetColumnData();
                f64* col2_ptr = (f64*)(col2_obj.GetData());
                for(SizeT row = 0; row < row_count; ++ row) {
                    EXPECT_FLOAT_EQ(col2_ptr[row], row % 8192);
                }
            }
        }

        {
            // Txn2: Rollback, OK
            new_txn->RollbackTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());
        }
    }

    {
        // Txn3: Create, OK
        new_txn = txn_mgr.CreateTxn();

        // Txn3: Begin, OK
        new_txn->BeginTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());

        {
            // Get column 0 and column 2 from global storage;
            Vector<ColumnID> column_ids{0, 2};

            UniquePtr<MetaTableState> read_table_meta = new_txn->GetTableMeta("db1", "tbl1", column_ids);
            EXPECT_EQ(read_table_meta->local_blocks_.size(), 0);
            EXPECT_EQ(read_table_meta->segment_map_.size(), 1);
            for (const auto &segment_pair: read_table_meta->segment_map_) {
                EXPECT_EQ(segment_pair.first, 0);
                EXPECT_NE(segment_pair.second.data_segment_, nullptr);
                EXPECT_EQ(segment_pair.second.column_data_map_.size(), 2);
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(0));
                EXPECT_TRUE(segment_pair.second.column_data_map_.contains(2));
                ColumnData *column0 = segment_pair.second.column_data_map_.at(0).column_data_;
                ColumnData *column2 = segment_pair.second.column_data_map_.at(2).column_data_;

                SizeT row_count = segment_pair.second.data_segment_->RowCount();
                ObjectHandle col0_obj = column0->GetColumnData();
                i8 *col0_ptr = (i8 *) (col0_obj.GetData());
                for (SizeT row = 0; row < row_count; ++row) {
//                LOG_TRACE("COL0 ROW: {}, value: {}", row, (i16)(col0_ptr[row]));
                    EXPECT_EQ(col0_ptr[row], (i8) (row));
                }

                ObjectHandle col2_obj = column2->GetColumnData();
                f64 *col2_ptr = (f64 *) (col2_obj.GetData());
                for (SizeT row = 0; row < row_count; ++row) {
                    EXPECT_FLOAT_EQ(col2_ptr[row], row % 8192);
                }
            }
        }

        // Txn3: Commit, OK
        new_txn->CommitTxn(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    }

#endif
}