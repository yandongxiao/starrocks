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

package com.starrocks.server;

import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class WarehouseManagerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    NodeMgr nodeMgr;

    @Mocked
    SystemInfoService systemInfo;

    @Test
    public void testWarehouseNotExist() {
        WarehouseManager mgr = new WarehouseManager();
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getWarehouse("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getWarehouse(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getAllComputeNodeIds("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getAllComputeNodeIds(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getComputeNodeId("a", null));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getComputeNodeId(1L, null));
    }

    @Test
    public void testGetAliveComputeNodes() throws UserException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == 10003L) {
                    ComputeNode node = new ComputeNode();
                    node.setAlive(false);
                    return node;
                }
                ComputeNode node = new ComputeNode();
                node.setAlive(true);
                return node;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = Lists.newArrayList(10003L, 10004L);
            }
        };

        WarehouseManager mgr = new WarehouseManager();
        mgr.initDefaultWarehouse();

        List<Long> nodeIds = mgr.getAllComputeNodeIds(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(2, nodeIds.size());

        List<ComputeNode> nodes = mgr.getAliveComputeNodes(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, nodes.size());
    }
}
