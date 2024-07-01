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
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SystemVariable;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// UserProperty is a class that represents the properties that are identified.
public class UserProperty {
    private static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    private static final String PROP_DEFAULT_SESSION_DATABASE = "default_session_database";
    private static final String PROP_DEFAULT_SESSION_CATALOG = "default_session_catalog";
    private static final String PROP_SESSION_PREFIX = "session.";

    @SerializedName(value = "m")
    private long maxConn = 1024;

    @SerializedName(value = "database")
    private String defaultSessionDatabase = "";

    @SerializedName(value = "catalog")
    private String defaultSessionCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

    @SerializedName(value = "sessionVariables")
    private Map<String, SystemVariable> sessionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public void update(List<Pair<String, String>> properties) throws DdlException {
        // update
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                maxConn = parseMaxConn(value);
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_DATABASE)) {
                // we do not check database existence here, because we should check catalog existence first.
                defaultSessionDatabase = value;
            } else if (key.equalsIgnoreCase(PROP_DEFAULT_SESSION_CATALOG)) {
                if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(value)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, value);
                }
                defaultSessionCatalog = value;
            } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                SystemVariable variable = new SystemVariable(sessionKey, new StringLiteral(value));
                VariableMgr.checkSystemVariableExist(variable);
                sessionVariables.put(sessionKey, variable);
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }

        // check whether the default session database exists
        if (!defaultSessionDatabase.isEmpty()) {
            String catalog = defaultSessionCatalog;
            if (catalog.equals("")) {
                catalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            }
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalog, defaultSessionDatabase);
            if (db == null) {
                throw new StarRocksConnectorException("db: " + defaultSessionDatabase + " not exists");
            }
        }
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

    public Map<String, SystemVariable> getSessionVariables() {
        return sessionVariables;
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
}
