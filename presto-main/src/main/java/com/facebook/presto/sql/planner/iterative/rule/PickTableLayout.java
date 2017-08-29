/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.optimizations.TableLayoutRewriter;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PickTableLayout
        implements RuleSet
{
    private final ImmutableSet<Rule<?>> rules;

    public PickTableLayout(Metadata metadata)
    {
        rules = ImmutableSet.of(
                new PickTableLayoutForPredicate(metadata),
                new PickTableLayoutWithoutPredicate(metadata));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return rules;
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;

        private PickTableLayoutForPredicate(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        private static final Pattern<FilterNode> PATTERN = filter();

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Optional<PlanNode> apply(FilterNode filterNode, Captures captures, Context context)
        {
            PlanNode source = context.getLookup().resolve(filterNode.getSource());

            if (!((source instanceof TableScanNode) && shouldRewriteTableLayout((TableScanNode) source))) {
                return Optional.empty();
            }

            TableLayoutRewriter tableLayoutRewriter = new TableLayoutRewriter(metadata, context.getSession(), context.getSymbolAllocator(), context.getIdAllocator());
            PlanNode rewrittenTableScan = tableLayoutRewriter.planTableScan((TableScanNode) source, filterNode.getPredicate());

            if (rewrittenTableScan instanceof TableScanNode || rewrittenTableScan instanceof ValuesNode || (((FilterNode) rewrittenTableScan).getPredicate() != filterNode.getPredicate())) {
                return Optional.of(rewrittenTableScan);
            }

            return Optional.empty();
        }

        private boolean shouldRewriteTableLayout(TableScanNode source)
        {
            return !source.getLayout().isPresent() || source.getOriginalConstraint() == BooleanLiteral.TRUE_LITERAL;
        }
    }

    private static final class PickTableLayoutWithoutPredicate
            implements Rule<TableScanNode>
    {
        private final Metadata metadata;

        private PickTableLayoutWithoutPredicate(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        private static final Pattern<TableScanNode> PATTERN = tableScan();

        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Optional<PlanNode> apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            if (tableScanNode.getLayout().isPresent()) {
                return Optional.empty();
            }

            TableLayoutRewriter tableLayoutRewriter = new TableLayoutRewriter(metadata, context.getSession(), context.getSymbolAllocator(), context.getIdAllocator());
            return Optional.of(tableLayoutRewriter.planTableScan(tableScanNode, BooleanLiteral.TRUE_LITERAL));
        }
    }
}
