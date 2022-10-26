//
// Created by JinHai on 2022/7/26.
//

#pragma once

#include "main/query_context.h"
#include "physical_operator_type.h"
#include "storage/table.h"

#include <memory>
#include <utility>

namespace infinity {

class OperatorPipeline;

class PhysicalOperator : public std::enable_shared_from_this<PhysicalOperator> {

public:
    explicit PhysicalOperator(
            PhysicalOperatorType type,
            std::shared_ptr<PhysicalOperator> left,
            std::shared_ptr<PhysicalOperator> right,
            uint64_t id)
            : operator_id_(id),
            operator_type_(type),
            left_(std::move(left)),
            right_(std::move(right)) {}
    virtual ~PhysicalOperator() = 0;

    virtual void
    Init() = 0;

    std::shared_ptr<OperatorPipeline> GenerateOperatorPipeline();

    std::shared_ptr<PhysicalOperator> left() const { return left_; }
    std::shared_ptr<PhysicalOperator> right() const { return right_; }
    uint64_t operator_id() const { return operator_id_; }

    virtual void
    Execute(std::shared_ptr<QueryContext>& query_context) = 0;

    std::shared_ptr<Table> output() const { return output_; }
protected:
    uint64_t operator_id_;
    PhysicalOperatorType operator_type_{PhysicalOperatorType::kInvalid};
    std::shared_ptr<PhysicalOperator> left_{nullptr};
    std::shared_ptr<PhysicalOperator> right_{nullptr};

    std::weak_ptr<OperatorPipeline> operator_pipeline_;
    std::shared_ptr<Table> output_;
};


}

