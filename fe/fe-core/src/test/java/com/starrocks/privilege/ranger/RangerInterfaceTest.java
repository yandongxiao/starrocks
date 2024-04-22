// Copyright 2021-present StarRocks, Inc. All rights reserved.
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
package com.starrocks.privilege.ranger;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerInterfaceTest {

    ConnectContext connectContext;
    StarRocksAssert starRocksAssert;

    @Before
    public void setUp() throws Exception {
        new Expectations() {
            {
            }
        };

        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db").useDatabase("db").withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testMaskingNull() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", Type.INT));

        RangerStarRocksAccessController connectScheduler = new RangerStarRocksAccessController();
        try {
            connectScheduler.getColumnMaskingPolicy(connectContext, tableName, columns);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testAccessControlProvider() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };

        AccessControlProvider accessControlProvider = new AccessControlProvider(null, new NativeAccessController());
        accessControlProvider.removeAccessControl("hive");

        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.removeAccessControl("hive");

        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);

        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);
    }

    @Test
    public void testMaskingExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_NULL);
                return result;
            }
        };

        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", Type.INT));

        Map<String, Expr> e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assert.assertTrue(new ArrayList<>(e.values()).get(0) instanceof NullLiteral);

        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_CUSTOM);
                result.setMaskedValue("v + 1");
                return result;
            }
        };

        e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assert.assertTrue(new ArrayList<>(e.values()).get(0) instanceof ArithmeticExpr);
    }

    @Test
    public void testRowAccessExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setFilterExpr("v1 = 1");
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");

        Expr rowFilter = rangerStarRocksAccessController.getRowAccessPolicy(connectContext, tableName);
        Assert.assertTrue(rowFilter instanceof BinaryPredicate);
    }

    @Test
    public void testPermission() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);

        try {
            rangerStarRocksAccessController.hasPermission(
                    RangerStarRocksResource.builder().setSystem().build(), UserIdentity.ROOT, PrivilegeType.OPERATE);
        } catch (Exception e) {
            Assert.fail();
        }

        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        Assert.assertThrows(AccessDeniedException.class, () -> rangerStarRocksAccessController.hasPermission(
                RangerStarRocksResource.builder().setSystem().build(), UserIdentity.ROOT, PrivilegeType.OPERATE));
    }

    @Test
    public void testRewriteWithAlias() throws Exception {
        Expr e = SqlParser.parseSqlToExpr("v4+1", new ConnectContext().getSessionVariable().getSqlMode());
        HashMap<String, Expr> exprHashMap = new HashMap<>();
        exprHashMap.put("v4", e);

        try (MockedStatic<Authorizer> authorizerMockedStatic = Mockito.mockStatic(Authorizer.class)) {
            authorizerMockedStatic.when(() -> Authorizer.getColumnMaskingPolicy(Mockito.any(),
                    Mockito.any(), Mockito.any())).thenReturn(exprHashMap);

            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t1 t", connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;

            Assert.assertTrue(((SelectRelation) queryStatement.getQueryRelation()).getRelation() instanceof SubqueryRelation);
        }
    }
}
