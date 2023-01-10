//
// Created by jinhai on 23-1-7.
//

#include <gtest/gtest.h>
#include "base_test.h"
#include "common/column_vector/column_vector.h"
#include "common/types/value.h"
#include "main/logger.h"
#include "main/stats/global_resource_usage.h"
#include "common/types/info/varchar_info.h"
#include "storage/catalog.h"
#include "function/scalar/mul.h"
#include "function/scalar_function_set.h"
#include "expression/column_expression.h"

class MulFunctionsTest : public BaseTest {
    void
    SetUp() override {
        infinity::Logger::Initialize();
        infinity::GlobalResourceUsage::Init();
    }

    void
    TearDown() override {
        infinity::Logger::Shutdown();
        EXPECT_EQ(infinity::GlobalResourceUsage::GetObjectCount(), 0);
        EXPECT_EQ(infinity::GlobalResourceUsage::GetRawMemoryCount(), 0);
        infinity::GlobalResourceUsage::UnInit();
    }
};

TEST_F(MulFunctionsTest, add_func) {
    using namespace infinity;

    UniquePtr<Catalog> catalog_ptr = MakeUnique<Catalog>();

    RegisterMulFunction(catalog_ptr);

    SharedPtr<FunctionSet> function_set = catalog_ptr->GetFunctionSetByName("*");
    EXPECT_EQ(function_set->type_, FunctionType::kScalar);
    SharedPtr<ScalarFunctionSet> scalar_function_set = std::static_pointer_cast<ScalarFunctionSet>(function_set);

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kTinyInt);
        DataType result_type(LogicalType::kTinyInt);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                "t1",
                                                                                "c1",
                                                                                0,
                                                                                0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(TinyInt, TinyInt)->TinyInt", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeTinyInt(static_cast<i8>(i)));
            data_block.AppendValue(1, Value::MakeTinyInt(static_cast<i8>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kTinyInt);
            EXPECT_EQ(v2.type_.type(), LogicalType::kTinyInt);
            EXPECT_EQ(v1.value_.tiny_int, static_cast<i8>(i));
            EXPECT_EQ(v2.value_.tiny_int, static_cast<i8>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kTinyInt);
            i16 res = static_cast<i16>(static_cast<i8>(i)) * static_cast<i16>(static_cast<i8>(i));
            if(res < std::numeric_limits<i8>::min() || res > std::numeric_limits<i8>::max()) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_EQ(v.value_.tiny_int, static_cast<i8>(res));
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kSmallInt);
        DataType result_type(LogicalType::kSmallInt);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(SmallInt, SmallInt)->SmallInt", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeSmallInt(static_cast<i16>(i)));
            data_block.AppendValue(1, Value::MakeSmallInt(static_cast<i16>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kSmallInt);
            EXPECT_EQ(v2.type_.type(), LogicalType::kSmallInt);
            EXPECT_EQ(v1.value_.small_int, static_cast<i16>(i));
            EXPECT_EQ(v2.value_.small_int, static_cast<i16>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kSmallInt);
            i32 res = static_cast<i32>(static_cast<i16>(i)) * static_cast<i32>(static_cast<i16>(i));
            if(res < std::numeric_limits<i16>::min() || res > std::numeric_limits<i16>::max()) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_EQ(v.value_.small_int, static_cast<i16>(res));
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kInteger);
        DataType result_type(LogicalType::kInteger);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Integer, Integer)->Integer", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeInt(static_cast<i32>(i)));
            data_block.AppendValue(1, Value::MakeInt(static_cast<i32>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kInteger);
            EXPECT_EQ(v2.type_.type(), LogicalType::kInteger);
            EXPECT_EQ(v1.value_.integer, static_cast<i32>(i));
            EXPECT_EQ(v2.value_.integer, static_cast<i32>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kInteger);
            i64 res = static_cast<i64>(static_cast<i32>(i)) * static_cast<i64>(static_cast<i32>(i));
            if(res < std::numeric_limits<i32>::min() || res > std::numeric_limits<i32>::max()) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_EQ(v.value_.integer, static_cast<i32>(res));
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kBigInt);
        DataType result_type(LogicalType::kBigInt);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(BigInt, BigInt)->BigInt", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeBigInt(static_cast<i64>(i)));
            data_block.AppendValue(1, Value::MakeBigInt(static_cast<i64>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kBigInt);
            EXPECT_EQ(v2.type_.type(), LogicalType::kBigInt);
            EXPECT_EQ(v1.value_.big_int, static_cast<i64>(i));
            EXPECT_EQ(v2.value_.big_int, static_cast<i64>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kBigInt);
            i64 res;
            if(__builtin_mul_overflow(static_cast<i64>(i), static_cast<i64>(i), &res)) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_EQ(v.value_.big_int, static_cast<i64>(res));
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kFloat);
        DataType result_type(LogicalType::kFloat);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Float, Float)->Float", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeFloat(static_cast<f32>(i)));
            data_block.AppendValue(1, Value::MakeFloat(static_cast<f32>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kFloat);
            EXPECT_EQ(v2.type_.type(), LogicalType::kFloat);
            EXPECT_FLOAT_EQ(v1.value_.float32, static_cast<f32>(i));
            EXPECT_FLOAT_EQ(v2.value_.float32, static_cast<f32>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kFloat);
            f32 res = static_cast<f32>(i) * static_cast<f32>(i);
            if(std::isinf(res) || std::isnan(res)) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_FLOAT_EQ(v.value_.float32, res);
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kDouble);
        DataType result_type(LogicalType::kDouble);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Double, Double)->Double", func.ToString().c_str());

        std::vector<DataType> column_types;
        column_types.emplace_back(data_type);
        column_types.emplace_back(data_type);

        size_t row_count = DEFAULT_VECTOR_SIZE;

        DataBlock data_block;
        data_block.Init(column_types);

        for (size_t i = 0; i < row_count; ++i) {
            data_block.AppendValue(0, Value::MakeDouble(static_cast<f64>(i)));
            data_block.AppendValue(1, Value::MakeDouble(static_cast<f64>(i)));
        }
        data_block.Finalize();

        for (size_t i = 0; i < row_count; ++i) {
            Value v1 = data_block.GetValue(0, i);
            Value v2 = data_block.GetValue(0, i);
            EXPECT_EQ(v1.type_.type(), LogicalType::kDouble);
            EXPECT_EQ(v2.type_.type(), LogicalType::kDouble);
            EXPECT_FLOAT_EQ(v1.value_.float64, static_cast<f64>(i));
            EXPECT_FLOAT_EQ(v2.value_.float64, static_cast<f64>(i));
        }

        ColumnVector result(result_type);
        result.Initialize();
        func.function_(data_block, result);

        for (size_t i = 0; i < row_count; ++i) {
            Value v = result.GetValue(i);
            EXPECT_EQ(v.type_.type(), LogicalType::kDouble);
            f64 res = static_cast<f64>(i) * static_cast<f64>(i);
            if(std::isinf(res) || std::isnan(res)) {
                EXPECT_FALSE(result.nulls_ptr_->IsTrue(i));
            } else {
                EXPECT_FLOAT_EQ(v.value_.float64, res);
            }
        }
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kDecimal16);
        DataType result_type(LogicalType::kDecimal16);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Decimal16, Decimal16)->Decimal16", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kDecimal32);
        DataType result_type(LogicalType::kDecimal32);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Decimal32, Decimal32)->Decimal32", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kDecimal64);
        DataType result_type(LogicalType::kDecimal64);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Decimal64, Decimal64)->Decimal64", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data_type(LogicalType::kDecimal128);
        DataType result_type(LogicalType::kDecimal128);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Decimal128, Decimal128)->Decimal128", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data1_type(LogicalType::kMixed);
        DataType data2_type(LogicalType::kBigInt);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data1_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data2_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Heterogeneous, BigInt)->Heterogeneous", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data1_type(LogicalType::kBigInt);
        DataType data2_type(LogicalType::kMixed);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data1_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data2_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(BigInt, Heterogeneous)->Heterogeneous", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data1_type(LogicalType::kMixed);
        DataType data2_type(LogicalType::kDouble);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data1_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data2_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Heterogeneous, Double)->Heterogeneous", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data1_type(LogicalType::kDouble);
        DataType data2_type(LogicalType::kMixed);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data1_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data2_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Double, Heterogeneous)->Heterogeneous", func.ToString().c_str());
    }

    {
        Vector<SharedPtr<BaseExpression>> inputs;

        DataType data1_type(LogicalType::kMixed);
        DataType data2_type(LogicalType::kMixed);
        SharedPtr<ColumnExpression> col1_expr_ptr = MakeShared<ColumnExpression>(data1_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);
        SharedPtr<ColumnExpression> col2_expr_ptr = MakeShared<ColumnExpression>(data2_type,
                                                                                 "t1",
                                                                                 "c1",
                                                                                 0,
                                                                                 0);

        inputs.emplace_back(col1_expr_ptr);
        inputs.emplace_back(col2_expr_ptr);

        ScalarFunction func = scalar_function_set->GetMostMatchFunction(inputs);
        EXPECT_STREQ("*(Heterogeneous, Heterogeneous)->Heterogeneous", func.ToString().c_str());
    }
}