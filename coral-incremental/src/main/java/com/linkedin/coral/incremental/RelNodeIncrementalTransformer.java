/**
 * Copyright 2023-2026 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;


public class RelNodeIncrementalTransformer {

  private static final String INSERT_DELTA_SUFFIX = "_delta";
  private static final String DELETE_DELTA_SUFFIX = "_delete_delta";

  private RelNodeIncrementalTransformer() {
  }

  public static RelNode convertRelIncremental(RelNode originalNode) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        return createDeltaTableScan(scan, INSERT_DELTA_SUFFIX);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode incrementalLeft = convertRelIncremental(left);
        RelNode incrementalRight = convertRelIncremental(right);
        RelNode deleteDeltaLeft = convertRelDeleteDelta(left);
        RelNode deleteDeltaRight = convertRelDeleteDelta(right);

        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        LogicalProject insertP1 = createProjectOverJoin(join, left, incrementalRight, rexBuilder);
        LogicalProject insertP2 = createProjectOverJoin(join, incrementalLeft, right, rexBuilder);
        LogicalProject insertP3 = createProjectOverJoin(join, incrementalLeft, incrementalRight, rexBuilder);

        LogicalProject deleteP1 = createProjectOverJoin(join, left, deleteDeltaRight, rexBuilder);
        LogicalProject deleteP2 = createProjectOverJoin(join, deleteDeltaLeft, right, rexBuilder);
        LogicalProject deleteP3 = createProjectOverJoin(join, deleteDeltaLeft, deleteDeltaRight, rexBuilder);

        RelNode insertBranches = LogicalUnion
            .create(Arrays.asList(LogicalUnion.create(Arrays.asList(insertP1, insertP2), true), insertP3), true);
        RelNode deleteBranches = LogicalUnion
            .create(Arrays.asList(LogicalUnion.create(Arrays.asList(deleteP1, deleteP2), true), deleteP3), true);
        return LogicalUnion.create(Arrays.asList(insertBranches, deleteBranches), true);
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode input = filter.getInput();
        if (input instanceof TableScan) {
          TableScan scan = (TableScan) input;
          RelNode filteredInsertDelta =
              LogicalFilter.create(createDeltaTableScan(scan, INSERT_DELTA_SUFFIX), filter.getCondition());
          RelNode filteredDeleteDelta =
              LogicalFilter.create(createDeltaTableScan(scan, DELETE_DELTA_SUFFIX), filter.getCondition());
          return LogicalUnion.create(Arrays.asList(filteredInsertDelta, filteredDeleteDelta), true);
        }
        RelNode transformedChild = convertRelIncremental(input);
        return LogicalFilter.create(transformedChild, filter.getCondition());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode transformedChild = convertRelIncremental(project.getInput());
        return LogicalProject.create(transformedChild, project.getProjects(), project.getRowType());
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        List<RelNode> transformedChildren =
            children.stream().map(child -> convertRelIncremental(child)).collect(Collectors.toList());
        return LogicalUnion.create(transformedChildren, union.all);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode transformedChild = convertRelIncremental(aggregate.getInput());
        return LogicalAggregate.create(transformedChild, aggregate.getGroupSet(), aggregate.getGroupSets(),
            aggregate.getAggCallList());
      }
    };
    return originalNode.accept(converter);
  }

  private static RelNode convertRelDeleteDelta(RelNode originalNode) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        return createDeltaTableScan(scan, DELETE_DELTA_SUFFIX);
      }
    };
    return originalNode.accept(converter);
  }

  private static RelNode createDeltaTableScan(TableScan scan, String deltaSuffix) {
    RelOptTable originalTable = scan.getTable();
    List<String> incrementalNames = new ArrayList<>(originalTable.getQualifiedName());
    String deltaTableName = incrementalNames.remove(incrementalNames.size() - 1) + deltaSuffix;
    incrementalNames.add(deltaTableName);
    RelOptTable incrementalTable =
        RelOptTableImpl.create(originalTable.getRelOptSchema(), originalTable.getRowType(), incrementalNames, null);
    return LogicalTableScan.create(scan.getCluster(), incrementalTable);
  }

  private static LogicalProject createProjectOverJoin(LogicalJoin join, RelNode left, RelNode right,
      RexBuilder rexBuilder) {
    LogicalJoin incrementalJoin =
        LogicalJoin.create(left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
    ArrayList<RexNode> projects = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    IntStream.range(0, incrementalJoin.getRowType().getFieldList().size()).forEach(i -> {
      projects.add(rexBuilder.makeInputRef(incrementalJoin, i));
      names.add(incrementalJoin.getRowType().getFieldNames().get(i));
    });
    return LogicalProject.create(incrementalJoin, projects, names);
  }

}
