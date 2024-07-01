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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// UserProperty is a class that represents the properties that are identified.
public class UserProperty {
    private static final Logger LOG = LogManager.getLogger(UserProperty.class);

    public static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    public static final String PROP_DEFAULT_DATABASE = "default_session_database";
    public static final String PROP_DEFAULT_CATALOG = "default_session_catalog";
    public static final String PROP_SESSION_PREFIX = "session.";

    public static final long MAX_CONN_DEFAULT_VALUE = 1024;
    public static final String DATABASE_DEFAULT_VALUE = "";
    public static final String CATALOG_DEFAULT_VALUE = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    public static final String NULL_VALUE = "null";

    @SerializedName(value = "m")
    private long maxConn = MAX_CONN_DEFAULT_VALUE;

    @SerializedName(value = "d")
    private String database = DATABASE_DEFAULT_VALUE;

    @SerializedName(value = "c")
    private String catalog = CATALOG_DEFAULT_VALUE;

    @SerializedName(value = "s")
    private Map<String, String> sessionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public void update(String userName, List<Pair<String, String>> properties) throws DdlException {
        UserIdentity user = getUserIdentityByName(userName);
        update(user, properties);
    }

    // update the user properties
    // we should check the properties and throw exceptions if the properties are invalid
    public void update(UserIdentity user, List<Pair<String, String>> properties) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        Set<Long> roleIds = new HashSet<>();
        try {
            roleIds = getAllRoleIds(user);
        } catch (PrivilegeException e) {
            // When user are being created, the user has no roles.
            LOG.info("failed to get role ids for user: {}", user, e);
        }

        String originalDatabase = database;
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                long newMaxConn = checkMaxConn(value);
                setMaxConn(newMaxConn);
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_DATABASE)) {
                // we do not check database existence here, because we should
                // check catalog existence first.
                setDatabase(value);
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_CATALOG)) {
                checkCatalog(user, roleIds, value);
                setCatalog(value);
            } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                checkSessionVariable(sessionKey, value);
                setSessionVariable(sessionKey, value);
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }
        checkDatabase(user, roleIds, originalDatabase);
    }

    // We do not check the variable default_session_database and default_session_catalog here, because we have checked them
    // when set properties. And we never should throw exceptions, this may cause the system can be started normally.
    public void updateForReplayJournal(List<Pair<String, String>> properties) {
        try {
            for (Pair<String, String> entry : properties) {
                String key = entry.first;
                String value = entry.second;
                updateProperty(key, value);
            }
        } catch (Exception e) {
            // we should never throw an exception when replaying journal
            LOG.warn("update user property from journal failed: ", e);
        }
    }

    // We can not make sure the set variables are all valid. Even if some variables are invalid, we should let user continue
    // to execute SQL.
    public void updateSessionContext(ConnectContext context) {
        try {
            // set catalog and database
            if (catalog.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                if (!database.isEmpty()) {
                    context.changeCatalogDb(getCatalogDbName());
                }
            } else {
                if (database.isEmpty()) {
                    context.changeCatalog(catalog);
                } else {
                    context.changeCatalogDb(getCatalogDbName());
                }
            }

            // set session variables
            for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
                SystemVariable variable = new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue()));
                context.modifySystemVariable(variable, true);
            }
        } catch (Exception e) {
            LOG.warn("set session env failed: ", e);
            // NOTE: it does not take effect
            context.getState().setOk(0L, 0,
                    String.format("set session variables from user property failed: %s", e.getMessage()));
        }
    }

    public String getCatalogDbName() {
        return catalog + "." + database;
    }

    public long getMaxConn() {
        return maxConn;
    }

    public String getDefaultDatabase() {
        return database;
    }

    public String getDefaultCatalog() {
        return catalog;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void setDefaultDatabase(String defaultSessionDatabase) {
        this.database = defaultSessionDatabase;
    }

    public void setDefaultCatalog(String defaultSessionCatalog) {
        this.catalog = defaultSessionCatalog;
    }

    public void setSessionVariables(Map<String, String> sessions) {
        this.sessionVariables = sessions;
    }

    // check the session variable
    private void checkSessionVariable(String sessionKey, String value) throws DdlException {
        if (value.equalsIgnoreCase(NULL_VALUE)) {
            return;
        }
        // check whether the variable exists
        SystemVariable variable = new SystemVariable(sessionKey, new StringLiteral(value));
        VariableMgr.checkSystemVariableExist(variable);

        // check whether the value is valid
        Field field = VariableMgr.getField(sessionKey);
        if (field == null || !canAssignValue(field, value)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, value);
        }

        // check flags of the variable, e.g. whether the variable is read-only
        VariableMgr.checkUpdate(variable);
    }

    // check whether the user has the privilege to access the catalog
    private void checkCatalog(UserIdentity user, Set<Long> roleIds, String value) {
        if (value.equalsIgnoreCase(NULL_VALUE)) {
            return;
        }

        try {
            if (!CatalogMgr.isInternalCatalog(value)) {
                Authorizer.checkAnyActionOnCatalog(user, roleIds, value);
                if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(value)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, value);
                }
            }
        } catch (AccessDeniedException | DdlException e) {
            LOG.info("failed to check authorization for catalog: {}", value, e);
            AccessDeniedException.reportAccessDenied(value, user, roleIds, PrivilegeType.ANY.name(), ObjectType.CATALOG.name(),
                    null);
        }
    }

    // check whether the user has the privilege to access the database
    // we need to reset the defaultSessionDatabase if it checks failed
    private void checkDatabase(UserIdentity user, Set<Long> roleIds, String originalDatabase) {
        if (database.equalsIgnoreCase(DATABASE_DEFAULT_VALUE)) {
            return;
        }

        try {
            MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
            Authorizer.checkAnyActionOnOrInDb(user, roleIds, catalog, database);

            // check whether the database exists
            Database db = metadataMgr.getDb(catalog, database);
            if (db == null) {
                String catalogDbName = getCatalogDbName();
                this.database = originalDatabase;
                throw new StarRocksConnectorException(catalogDbName + " not exists");
            }
        } catch (AccessDeniedException e) {
            this.database = originalDatabase;
            LOG.warn("failed to check authorization for database: {}", database, e);
            AccessDeniedException.reportAccessDenied(database, user, roleIds, PrivilegeType.ANY.name(),
                    ObjectType.DATABASE.name(), null);
        }
    }

    private UserIdentity getUserIdentityByName(String userName) {
        AuthenticationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        Map<UserIdentity, UserAuthenticationInfo> userToAuthInfo = authorizationMgr.getUserToAuthenticationInfo();
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity = userToAuthInfo.entrySet().stream()
                .filter(entry -> (entry.getKey().getUser().equals(userName)))
                .findFirst().orElse(null);
        if (matchedUserIdentity == null) {
            throw new SemanticException("Unknown user: " + userName);
        }

        return matchedUserIdentity.getKey();
    }

    // get all role ids of the user, including the default roles and the inactivated roles
    private Set<Long> getAllRoleIds(UserIdentity user) throws PrivilegeException {
        return GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(user);
    }

    public static List<Pair<String, String>> changeToPairList(Map<String, String> properties) {
        List<Pair<String, String>> list = Lists.newArrayList();
        if (properties == null || properties.size() == 0) {
            return list;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            list.add(Pair.create(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private boolean canAssignValue(Field field, String value) {
        Class<?> fieldType = field.getType();
        try {
            if (fieldType == int.class || fieldType == Integer.class) {
                Integer.parseInt(value);
            } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Invalid boolean value");
                }
            } else if (fieldType == byte.class || fieldType == Byte.class) {
                Byte.parseByte(value);
            } else if (fieldType == short.class || fieldType == Short.class) {
                Short.parseShort(value);
            } else if (fieldType == long.class || fieldType == Long.class) {
                Long.parseLong(value);
            } else if (fieldType == float.class || fieldType == Float.class) {
                Float.parseFloat(value);
            } else if (fieldType == double.class || fieldType == Double.class) {
                Double.parseDouble(value);
            } else if (fieldType == String.class) {
                return true;
            } else {
                return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void updateProperty(String key, String value) throws DdlException {
        if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
            long maxConn = checkMaxConn(value);
            setMaxConn(maxConn);
        } else if (key.equalsIgnoreCase(PROP_DEFAULT_DATABASE)) {
            setDatabase(value);
        } else if (key.equalsIgnoreCase(PROP_DEFAULT_CATALOG)) {
            setCatalog(value);
        } else if (key.startsWith(PROP_SESSION_PREFIX)) {
            String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
            setSessionVariable(sessionKey, value);
        }
    }

    private void setSessionVariable(String sessionKey, String value) {
        if (value.equalsIgnoreCase(NULL_VALUE)) {
            sessionVariables.remove(sessionKey);
        } else {
            sessionVariables.put(sessionKey, value);
        }
    }

    private void setDatabase(String sessionDatabase) {
        if (sessionDatabase.equalsIgnoreCase(NULL_VALUE)) {
            this.database = DATABASE_DEFAULT_VALUE;
        } else {
            this.database = sessionDatabase;
        }
    }

    private void setCatalog(String sessionCatalog) {
        if (sessionCatalog.equalsIgnoreCase(NULL_VALUE)) {
            this.catalog = CATALOG_DEFAULT_VALUE;
        } else {
            this.catalog = sessionCatalog;
        }
    }

    private long checkMaxConn(String value) throws DdlException {
        if (value.equalsIgnoreCase(NULL_VALUE)) {
            return MAX_CONN_DEFAULT_VALUE;
        }

        try {
            long newMaxConn = Long.parseLong(value);

            if (newMaxConn <= 0 || newMaxConn > 10000) {
                throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be between 1 and 10000");
            }

            if (newMaxConn > Config.qe_max_connection) {
                throw new DdlException(
                        PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be less than qe_max_connection(" +
                                Config.qe_max_connection + ")");
            }

            return newMaxConn;
        } catch (NumberFormatException e) {
            throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not a number");
        }
    }

    private void setMaxConn(long value) {
        maxConn = value;
    }
}
