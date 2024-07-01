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

package com.starrocks.authentication;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Pair;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserPropertyTest {

    private static String databaseName = "myDB";

    private static String catalogName = "myCatalog";

    private static ConnectContext connectContext;

    private static StarRocksAssert starRocksAssert;

    private static AuthorizationMgr authorizationManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);

        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);

        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();

        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();
        ctxToRoot();
    }

    private static void ctxToRoot() throws PrivilegeException {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setCurrentRoleIds(
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(UserIdentity.ROOT));

        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getUser());
    }

    @Before
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().clear();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        if (starRocksAssert.getCtx().getGlobalStateMgr().getCatalogMgr().catalogExists(catalogName)) {
            DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) UtFrameUtils.parseStmtWithNewParser(
                    String.format("DROP CATALOG IF EXISTS %s", catalogName), starRocksAssert.getCtx());
            starRocksAssert.getCtx().getGlobalStateMgr().getCatalogMgr().dropCatalog(dropCatalogStmt);
        }
    }

    @Test
    public void testUpdate_WithCatalog() throws Exception {
        UserProperty userProperty = new UserProperty();

        String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
        starRocksAssert.withCatalog(createExternalCatalog);

        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_CATALOG, catalogName));
        userProperty.update("root", properties);
    }

    @Test
    public void testUpdate_WithUser() throws Exception {
        try {
            UserProperty userProperty = new UserProperty();

            String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                    "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
            starRocksAssert.withCatalog(createExternalCatalog);

            String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
            CreateUserStmt createUserStmt =
                    (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

            AuthenticationMgr authenticationManager =
                    starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
            authenticationManager.createUser(createUserStmt);

            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
            properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_CATALOG, catalogName));
            userProperty.update("test", properties);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Access denied"));
        }

        AuthorizationMgr authorizationMgr = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();

        // we assign the user 'test' to the role 'admin'
        String createRoleSql = "CREATE ROLE r1";
        CreateRoleStmt createRoleStmt =
                (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(createRoleSql, starRocksAssert.getCtx());
        authorizationMgr.createRole(createRoleStmt);

        String grantRoleSql = "GRANT r1 TO USER test";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, starRocksAssert.getCtx());
        authorizationMgr.grantRole(grantRoleStmt);

        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant CREATE DATABASE on CATALOG myCatalog to role r1",
                starRocksAssert.getCtx());
        authorizationMgr.grant(grantPrivilegeStmt);

        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_CATALOG, catalogName));
        userProperty.update("test", properties);
    }

    @Test
    public void testUpdate_withMultipleUpdate() throws Exception {
        List<Pair<String, String>> properties = new ArrayList<>();
        String createExternalCatalog = "CREATE EXTERNAL CATALOG myCatalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
        starRocksAssert.withCatalog(createExternalCatalog);

        UserProperty userProperty = new UserProperty();
        properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
        userProperty.update("root", properties);

        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_CATALOG, catalogName));
        userProperty.update("root", properties);

        Assert.assertEquals(2000, userProperty.getMaxConn());
        Assert.assertEquals(catalogName, userProperty.getDefaultSessionCatalog());
    }

    @Test
    public void testUpdate_WithDatabase() throws Exception {
        starRocksAssert.withDatabase(databaseName);

        UserProperty userProperty = new UserProperty();
        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_DATABASE, databaseName));
        userProperty.update("root", properties);
    }

    @Test
    public void testUpdate_WithSessionVariables() throws Exception {
        try {
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.aaa", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
        }

        try {
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.wait_timeout", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
        }

        try {
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.init_connect", "bbb"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
        }

        try {
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.system_time_zone", "Asia/Shanghai"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
            Assert.assertEquals(1, 1);
        }

        try {
            UserProperty userProperty = new UserProperty();
            List<Pair<String, String>> properties = new ArrayList<>();
            properties.add(new Pair<>("session.wait_timeout", "1000"));
            userProperty.update("root", properties);
            Assert.assertEquals(1, 1);
        } catch (Exception e) {
            Assert.assertEquals(1, 2);
        }
    }

    @Test
    public void testUpdateForReplayJournal() {
        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>(UserProperty.PROP_MAX_USER_CONNECTIONS, "2000"));
        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_DATABASE, "database"));
        properties.add(new Pair<>(UserProperty.PROP_DEFAULT_SESSION_CATALOG, "catalog"));
        properties.add(new Pair<>("session.aaa", "bbb"));
        properties.add(new Pair<>("xxx", "yyy"));

        UserProperty userProperty = new UserProperty();
        userProperty.updateForReplayJournal(properties);

        Assert.assertEquals(2000, userProperty.getMaxConn());
        Assert.assertEquals("database", userProperty.getDefaultSessionDatabase());
        Assert.assertEquals("catalog", userProperty.getDefaultSessionCatalog());
        Map<String, String> sessionVariables = userProperty.getSessionVariables();
        Assert.assertEquals(1, sessionVariables.size());
        Assert.assertEquals("bbb", sessionVariables.get("aaa"));
    }

    @Test
    public void testUpdateSessionContext_WithSomeAbnormalCases() {
        ConnectContext context = new ConnectContext(null);
        UserProperty userProperty = new UserProperty();
        userProperty.updateSessionContext(context);

        // mock some abnormal cases, and make sure all exceptions are caught

        userProperty.setDefaultSessionCatalog("catalog");
        userProperty.updateSessionContext(context);

        userProperty.setDefaultSessionCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        userProperty.setDefaultSessionCatalog("database");
        userProperty.updateSessionContext(context);

        userProperty.setDefaultSessionCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        userProperty.setDefaultSessionDatabase("");
        Map<String, String> sessionVariables = userProperty.getSessionVariables();
        sessionVariables.put("aaa", "bbb");
        userProperty.setSessionVariables(sessionVariables);
        userProperty.updateSessionContext(context);
    }

    @Test
    public void testGetCatalogDbName() {
        UserProperty userProperty = new UserProperty();
        userProperty.setDefaultSessionDatabase("db");
        userProperty.setDefaultSessionCatalog("catalog");
        String name = userProperty.getCatalogDbName();
        Assert.assertEquals("catalog.db", name);

    }

    @Test
    public void testGetMaxConn() {
        UserProperty userProperty = new UserProperty();
        long maxConnections = userProperty.getMaxConn();
        Assert.assertEquals(1024, maxConnections);
    }

    @Test
    public void testGetDefaultSessionDatabase() {
        UserProperty userProperty = new UserProperty();
        String defaultSessionDatabase = userProperty.getDefaultSessionDatabase();
        Assert.assertEquals("", defaultSessionDatabase);
    }

    @Test
    public void testGetDefaultSessionCatalog() {
        UserProperty userProperty = new UserProperty();
        String defaultSessionCatalog = userProperty.getDefaultSessionCatalog();
        Assert.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, defaultSessionCatalog);
    }

    @Test
    public void testGetSessionVariables() {
        UserProperty userProperty = new UserProperty();
        Map<String, String> sessionVariables = userProperty.getSessionVariables();
        Assert.assertEquals(0, sessionVariables.size());
    }
}
