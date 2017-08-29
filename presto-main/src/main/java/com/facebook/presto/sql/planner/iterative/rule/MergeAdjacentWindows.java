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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.dependsOn;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.window;

public class MergeAdjacentWindows
        implements Rule<WindowNode>
{
    private static final Capture<WindowNode> CHILD = newCapture();

    private static final Pattern<WindowNode> PATTERN = window()
            .with(source().matching(window().capturedAs(CHILD)));

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(WindowNode parent, Captures captures, Context context)
    {
        WindowNode child = captures.get(CHILD);

        if (!child.getSpecification().equals(parent.getSpecification()) || dependsOn(parent, child)) {
            return Optional.empty();
        }

        ImmutableMap.Builder<Symbol, WindowNode.Function> functionsBuilder = ImmutableMap.builder();
        functionsBuilder.putAll(parent.getWindowFunctions());
        functionsBuilder.putAll(child.getWindowFunctions());

        return Optional.of(new WindowNode(
                parent.getId(),
                child.getSource(),
                parent.getSpecification(),
                functionsBuilder.build(),
                parent.getHashSymbol(),
                parent.getPrePartitionedInputs(),
                parent.getPreSortedOrderPrefix()));
    }
}
