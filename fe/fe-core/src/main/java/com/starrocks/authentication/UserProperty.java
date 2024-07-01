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
    public static final String PROP_DEFAULT_SESSION_DATABASE = "default_session_database";
    public static final String PROP_DEFAULT_SESSION_CATALOG = "default_session_catalog";
    public static final String PROP_SESSION_PREFIX = "session.";

    @SerializedName(value = "m")
    private long maxConn = 1024;

    @SerializedName(value = "database")
    private String defaultSessionDatabase = "";

    @SerializedName(value = "catalog")
    private String defaultSessionCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

    @SerializedName(value = "sessionVariables")
    private Map<String, String> sessionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public void update(String userName, List<Pair<String, String>> properties) throws DdlException {
        UserIdentity user = getUserIdentityByName(userName);
        Set<Long> roleIds = new HashSet<>();

        try {
            roleIds = getAllRoleIds(user);
        } catch (PrivilegeException e) {
            LOG.warn("failed to get role ids for user: {}", userName, e);
        }

        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                maxConn = parseMaxConn(value);
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_DATABASE)) {
                // we do not check database existence here, because we should
                // check catalog existence first.
                defaultSessionDatabase = value;
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_CATALOG)) {
                checkCatalog(user, roleIds, value);
                defaultSessionCatalog = value;
            } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                checkSessionVariables(sessionKey, value);
                sessionVariables.put(sessionKey, value);
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }
        checkDatabase(user, roleIds, defaultSessionDatabase);
    }

    // We do not check the variable default_session_database and default_session_catalog here, because we have checked them
    // when set properties. And we never should throw exceptions, this may cause the system can be started normally.
    public void updateForReplayJournal(List<Pair<String, String>> properties) {
        try {
            for (Pair<String, String> entry : properties) {
                String key = entry.first;
                String value = entry.second;
                if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                    maxConn = parseMaxConn(value);
                } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_DATABASE)) {
                    defaultSessionDatabase = value;
                } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_CATALOG)) {
                    defaultSessionCatalog = value;
                } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                    String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                    sessionVariables.put(sessionKey, value);
                }
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
            if (defaultSessionCatalog.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                if (!defaultSessionDatabase.isEmpty()) {
                    context.changeCatalogDb(getCatalogDbName());
                }
            } else {
                if (defaultSessionDatabase.isEmpty()) {
                    context.changeCatalog(defaultSessionCatalog);
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
        return defaultSessionCatalog + "." + defaultSessionDatabase;
    }

    public long getMaxConn() {
        return maxConn;
    }

    public String getDefaultSessionDatabase() {
        return defaultSessionDatabase;
    }

    public String getDefaultSessionCatalog() {
        return defaultSessionCatalog;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void setDefaultSessionDatabase(String defaultSessionDatabase) {
        this.defaultSessionDatabase = defaultSessionDatabase;
    }

    public void setDefaultSessionCatalog(String defaultSessionCatalog) {
        this.defaultSessionCatalog = defaultSessionCatalog;
    }

    public void setSessionVariables(Map<String, String> sessions) {
        this.sessionVariables = sessions;
    }

    // check the session variable
    private void checkSessionVariables(String sessionKey, String value) throws DdlException {
        // check whether the variable exists
        SystemVariable variable = new SystemVariable(sessionKey, new StringLiteral(value));
        VariableMgr.checkSystemVariableExist(variable);

        // check whether the value is valid
        Field field = VariableMgr.getField(sessionKey);
        if (field == null || !canAssignValue(field, value)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, value);
        }

        // check flags of the variable, e.g. whether the variable is read-only
        VariableMgr.checkFlag(variable);
    }

    // check whether the user has the privilege to access the catalog
    private void checkCatalog(UserIdentity user, Set<Long> roleIds, String value) {
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
    private void checkDatabase(UserIdentity user, Set<Long> roleIds, String database) {
        if (database.isEmpty()) {
            return;
        }

        try {
            MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
            Authorizer.checkAnyActionOnOrInDb(user, roleIds, defaultSessionCatalog, database);

            // check whether the database exists
            Database db = metadataMgr.getDb(defaultSessionCatalog, database);
            if (db == null) {
                String catalogDbName = getCatalogDbName();
                defaultSessionDatabase = "";
                throw new StarRocksConnectorException(catalogDbName + " not exists");
            }
        } catch (AccessDeniedException e) {
            defaultSessionDatabase = "";
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

    private long parseMaxConn(String value) throws DdlException {
        long newMaxConn = maxConn;

        try {
            newMaxConn = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not a number");
        }

        if (newMaxConn <= 0 || newMaxConn > 10000) {
            throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be between 1 and 10000");
        }

        if (newMaxConn > Config.qe_max_connection) {
            throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be less than qe_max_connection(" +
                    Config.qe_max_connection + ")");
        }

        return newMaxConn;
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
}
