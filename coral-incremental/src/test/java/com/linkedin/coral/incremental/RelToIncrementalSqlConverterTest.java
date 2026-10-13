/**
 * Copyright 2023-2026 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import java.io.File;
import java.io.IOException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.coral.transformers.CoralRelToSqlNodeConverter;

import static com.linkedin.coral.incremental.TestUtils.*;
import static org.apache.calcite.sql.type.OperandTypes.*;
import static org.testng.Assert.*;


public class RelToIncrementalSqlConverterTest {

  private HiveConf conf;

  @BeforeClass
  public void beforeClass() throws HiveException, MetaException, IOException {
    conf = TestUtils.loadResourceHiveConf();
    TestUtils.initializeViews(conf);
  }

  @AfterTest
  public void afterClass() throws IOException {
    FileUtils.deleteDirectory(new File(conf.get(CORAL_INCREMENTAL_TEST_DIR)));
  }

  public String convert(RelNode relNode) {
    RelNode incrementalRelNode = RelNodeIncrementalTransformer.convertRelIncremental(relNode);
    CoralRelToSqlNodeConverter converter = new CoralRelToSqlNodeConverter();
    SqlNode sqlNode = converter.convert(incrementalRelNode);
    return sqlNode.toSqlString(converter.INSTANCE).getSql();
  }

  public String getIncrementalModification(String sql) {
    RelNode originalRelNode = hiveToRelConverter.convertSql(sql);
    return convert(originalRelNode);
  }

  @Test
  public void testSimpleSelectAll() {
    String sql = "SELECT * FROM test.foo";
    String expected = "SELECT *\n" + "FROM test.foo_delta AS foo_delta";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testSimpleJoin() {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    String expected = "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar1\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta ON bar1.x = bar2_delta.x\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delta AS bar1_delta\n" + "INNER JOIN test.bar2 AS bar2 ON bar1_delta.x = bar2.x) AS t\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar1_delta AS bar1_delta0\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x) AS t0\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar10\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta ON bar10.x = bar2_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta\n"
        + "INNER JOIN test.bar2 AS bar20 ON bar1_delete_delta.x = bar20.x) AS t1\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta0\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta0 ON bar1_delete_delta0.x = bar2_delete_delta0.x) AS t2";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testJoinWithFilter() {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x WHERE test.bar1.x > 10";
    String expected = "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n"
        + "FROM test.bar1 AS bar1\n" + "INNER JOIN test.bar2_delta AS bar2_delta ON bar1.x = bar2_delta.x\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar1_delta AS bar1_delta\n"
        + "INNER JOIN test.bar2 AS bar2 ON bar1_delta.x = bar2.x) AS t\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delta AS bar1_delta0\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x) AS t0\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar10\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta ON bar10.x = bar2_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta\n"
        + "INNER JOIN test.bar2 AS bar20 ON bar1_delete_delta.x = bar20.x) AS t1\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta0\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta0 ON bar1_delete_delta0.x = bar2_delete_delta0.x) AS t2) AS t3\n"
        + "WHERE t3.x > 10";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testJoinWithNestedFilter() {
    String sql =
        "WITH tmp AS (SELECT * from test.bar1 WHERE test.bar1.x > 10), tmp2 AS (SELECT * from test.bar2) SELECT * FROM tmp JOIN tmp2 ON tmp.x = tmp2.x";
    String expected = "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n"
        + "FROM test.bar1 AS bar1\n" + "WHERE bar1.x > 10) AS t\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta ON t.x = bar2_delta.x\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM (SELECT *\n" + "FROM test.bar1_delta AS bar1_delta\n" + "WHERE bar1_delta.x > 10\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta\n"
        + "WHERE bar1_delete_delta.x > 10) AS t2\n" + "INNER JOIN test.bar2 AS bar2 ON t2.x = bar2.x) AS t3\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1_delta AS bar1_delta0\n"
        + "WHERE bar1_delta0.x > 10\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta0\n" + "WHERE bar1_delete_delta0.x > 10) AS t6\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta0 ON t6.x = bar2_delta0.x) AS t7\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar10\n"
        + "WHERE bar10.x > 10) AS t8\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta ON t8.x = bar2_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta1\n"
        + "WHERE bar1_delete_delta1.x > 10) AS t9\n" + "INNER JOIN test.bar2 AS bar20 ON t9.x = bar20.x) AS t10\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta2\n"
        + "WHERE bar1_delete_delta2.x > 10) AS t11\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta0 ON t11.x = bar2_delete_delta0.x) AS t12";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testNestedJoin() {
    String sql =
        "WITH tmp AS (SELECT * FROM test.bar1 INNER JOIN test.bar2 ON test.bar1.x = test.bar2.x) SELECT * FROM tmp INNER JOIN test.bar3 ON tmp.x = test.bar3.x";
    String expected = "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar1\n"
        + "INNER JOIN test.bar2 AS bar2 ON bar1.x = bar2.x\n"
        + "INNER JOIN test.bar3_delta AS bar3_delta ON bar1.x = bar3_delta.x\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar10\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta ON bar10.x = bar2_delta.x\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delta AS bar1_delta\n" + "INNER JOIN test.bar2 AS bar20 ON bar1_delta.x = bar20.x) AS t\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar1_delta AS bar1_delta0\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x) AS t0\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar11\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta ON bar11.x = bar2_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta\n"
        + "INNER JOIN test.bar2 AS bar21 ON bar1_delete_delta.x = bar21.x) AS t1\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta0\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta0 ON bar1_delete_delta0.x = bar2_delete_delta0.x) AS t2) AS t3\n"
        + "INNER JOIN test.bar3 AS bar3 ON t3.x = bar3.x) AS t4\n" + "UNION ALL\n" + "SELECT *\n" + "FROM (SELECT *\n"
        + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar12\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta1 ON bar12.x = bar2_delta1.x\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delta AS bar1_delta1\n" + "INNER JOIN test.bar2 AS bar22 ON bar1_delta1.x = bar22.x) AS t5\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar1_delta AS bar1_delta2\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta2 ON bar1_delta2.x = bar2_delta2.x) AS t6\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar13\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta1 ON bar13.x = bar2_delete_delta1.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta1\n"
        + "INNER JOIN test.bar2 AS bar23 ON bar1_delete_delta1.x = bar23.x) AS t7\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta2\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta2 ON bar1_delete_delta2.x = bar2_delete_delta2.x) AS t8) AS t9\n"
        + "INNER JOIN test.bar3_delta AS bar3_delta0 ON t9.x = bar3_delta0.x) AS t10\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar14\n"
        + "INNER JOIN test.bar2 AS bar24 ON bar14.x = bar24.x\n"
        + "INNER JOIN test.bar3_delete_delta AS bar3_delete_delta ON bar14.x = bar3_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta3\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta3 ON bar1_delete_delta3.x = bar2_delete_delta3.x\n"
        + "INNER JOIN test.bar3 AS bar30 ON bar1_delete_delta3.x = bar30.x) AS t11\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta4\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta4 ON bar1_delete_delta4.x = bar2_delete_delta4.x\n"
        + "INNER JOIN test.bar3_delete_delta AS bar3_delete_delta0 ON bar1_delete_delta4.x = bar3_delete_delta0.x) AS t12";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testUnion() {
    String sql = "SELECT * FROM test.bar1 UNION SELECT * FROM test.bar2 UNION SELECT * FROM test.bar3";
    String expected =
        "SELECT t1.x, t1.y\n" + "FROM (SELECT t.x, t.y\n" + "FROM (SELECT *\n" + "FROM test.bar1_delta AS bar1_delta\n"
            + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar2_delta AS bar2_delta) AS t\n" + "GROUP BY t.x, t.y\n"
            + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar3_delta AS bar3_delta) AS t1\n" + "GROUP BY t1.x, t1.y";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testSelectSpecific() {
    String sql = "SELECT a FROM test.foo";
    String expected = "SELECT foo_delta.a\n" + "FROM test.foo_delta AS foo_delta";
    assertEquals(getIncrementalModification(sql), expected);
  }

  @Test
  public void testSelectSpecificJoin() {
    String sql = "SELECT test.bar2.y FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    String expected = "SELECT t3.y0 AS y\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n"
        + "FROM test.bar1 AS bar1\n" + "INNER JOIN test.bar2_delta AS bar2_delta ON bar1.x = bar2_delta.x\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar1_delta AS bar1_delta\n"
        + "INNER JOIN test.bar2 AS bar2 ON bar1_delta.x = bar2.x) AS t\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delta AS bar1_delta0\n"
        + "INNER JOIN test.bar2_delta AS bar2_delta0 ON bar1_delta0.x = bar2_delta0.x) AS t0\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM (SELECT *\n" + "FROM (SELECT *\n" + "FROM test.bar1 AS bar10\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta ON bar10.x = bar2_delete_delta.x\n" + "UNION ALL\n"
        + "SELECT *\n" + "FROM test.bar1_delete_delta AS bar1_delete_delta\n"
        + "INNER JOIN test.bar2 AS bar20 ON bar1_delete_delta.x = bar20.x) AS t1\n" + "UNION ALL\n" + "SELECT *\n"
        + "FROM test.bar1_delete_delta AS bar1_delete_delta0\n"
        + "INNER JOIN test.bar2_delete_delta AS bar2_delete_delta0 ON bar1_delete_delta0.x = bar2_delete_delta0.x) AS t2) AS t3";
    assertEquals(getIncrementalModification(sql), expected);
  }

  /**
   * Scenario: {@code SELECT a FROM test.foo WHERE a > 10}
   * <ul>
   *   <li>DELETE: a deleted base row must disappear from the materialized result</li>
   *   <li>UPDATE: a row changing from {@code a = 15} to {@code a = 5} must leave the result</li>
   * </ul>
   * Append-only {@code foo_delta} scans only add rows. Removals require a delete change stream
   * such as {@code foo_delete_delta}.
   */
  @Test
  public void testFilteredSelectShouldHandleDeletesAndUpdates() {
    String sql = "SELECT a FROM test.foo WHERE a > 10";
    String expected = "SELECT t1.a\n" + "FROM (SELECT *\n" + "FROM test.foo_delta AS foo_delta\n"
        + "WHERE foo_delta.a > 10\n" + "UNION ALL\n" + "SELECT *\n" + "FROM test.foo_delete_delta AS foo_delete_delta\n"
        + "WHERE foo_delete_delta.a > 10) AS t1";
    assertEquals(getIncrementalModification(sql), expected);
  }

  /**
   * Scenario: when rows are deleted from {@code test.bar2}, matching join results must be removed
   * from the materialized view. Insert-only {@code bar2_delta} join branches are not enough.
   */
  @Test
  public void testJoinShouldHandleDeletesAndUpdates() {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    String actual = getIncrementalModification(sql);
    assertTrue(actual.contains("bar2_delete_delta"),
        "Join incremental rewrite should reference delete deltas for removed rows, but was:\n" + actual);
  }
}
