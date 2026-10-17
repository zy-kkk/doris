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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Final per-group top-N result, unlike the window prefilter PartitionTopN.
 */
public class LogicalGroupTopN<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements PropagateFuncDeps {

    private final List<Expression> groupKeys;
    private final List<OrderKey> orderKeys;
    private final long limit;
    private final long offset;
    private final Supplier<List<Expression>> expressions;

    public LogicalGroupTopN(List<Expression> groupKeys, List<OrderKey> orderKeys, long limit, long offset,
            CHILD_TYPE child) {
        this(groupKeys, orderKeys, limit, offset, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Construct a final per-group result window.
     */
    public LogicalGroupTopN(List<Expression> groupKeys, List<OrderKey> orderKeys, long limit, long offset,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GROUP_TOP_N, groupExpression, logicalProperties, child);
        this.orderKeys = ImmutableList.copyOf(Objects.requireNonNull(orderKeys, "orderKeys can not be null"));
        this.groupKeys = ImmutableList.copyOf(groupKeys);
        this.limit = limit;
        this.offset = offset;
        this.expressions = Suppliers.memoize(() -> {
            ImmutableList.Builder<Expression> exprs = ImmutableList.builderWithExpectedSize(orderKeys.size());
            for (OrderKey orderKey : orderKeys) {
                exprs.add(orderKey.getExpr());
            }
            exprs.addAll(groupKeys);
            return exprs.build();
        });
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    public List<Expression> getGroupKeys() {
        return groupKeys;
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public long getOffset() {
        return offset;
    }

    public long getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalGroupTopN",
                "groupKeys", groupKeys,
                "limit", limit,
                "offset", offset,
                "orderKeys", orderKeys
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalGroupTopN<?> that = (LogicalGroupTopN<?>) o;
        return groupKeys.equals(that.groupKeys) && this.offset == that.offset && this.limit == that.limit
                && Objects.equals(this.orderKeys, that.orderKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupKeys, orderKeys, limit, offset);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalGroupTopN(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return expressions.get();
    }

    @Override
    public LogicalGroupTopN<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalGroupTopN should have 1 child, but input is %s", children.size());
        return new LogicalGroupTopN<>(groupKeys, orderKeys, limit, offset, children.get(0));
    }

    @Override
    public LogicalGroupTopN<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalGroupTopN<>(groupKeys, orderKeys, limit, offset, groupExpression,
                Optional.of(getLogicalProperties()),
                child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalGroupTopN<>(groupKeys, orderKeys, limit, offset, groupExpression,
                logicalProperties, children.get(0));
    }

}
