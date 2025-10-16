/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package cn.think.in.java;

import cn.think.in.java.common.NodeConfig;
import cn.think.in.java.constant.StateMachineSaveType;
import cn.think.in.java.impl.DefaultNode;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

/**
 * Raft节点启动类
 * 
 * 启动参数说明：
 * -DserverPort=8775  # 设置当前节点的端口号
 * -Dcluster.addr.list=localhost:8775,localhost:8776,localhost:8777  # 设置集群所有节点地址（可选）
 * 
 * 默认配置：
 * - 默认端口：8779
 * - 默认集群：5个节点（8775-8779）
 * - 状态机类型：RocksDB
 */
@Slf4j
public class RaftNodeBootStrap {

    public static void main(String[] args) throws Throwable {
        boot();
    }

    /**
     * 启动Raft节点
     * 
     * 启动流程：
     * 1. 解析启动参数，获取节点端口和集群地址
     * 2. 创建节点配置
     * 3. 初始化节点
     * 4. 等待关闭信号
     * 5. 优雅关闭节点
     */
    public static void boot() throws Throwable {
        // 获取集群地址列表
        String property = System.getProperty("cluster.addr.list");
        String[] peerAddr;

        if (StringUtil.isNullOrEmpty(property)) {
            // 默认5个节点的集群
            peerAddr = new String[]{"localhost:8775", "localhost:8776", "localhost:8777", "localhost:8778", "localhost:8779"};
        } else {
            peerAddr = property.split(",");
        }

        // 创建节点配置
        NodeConfig config = new NodeConfig();

        // 设置当前节点端口（从系统属性获取，默认为8779）
        config.setSelfPort(Integer.parseInt(System.getProperty("serverPort", "8779")));

        // 设置集群中所有节点的地址
        config.setPeerAddrs(Arrays.asList(peerAddr));
        // 设置状态机存储类型为RocksDB
        config.setStateMachineSaveType(StateMachineSaveType.ROCKS_DB);

        // 获取节点实例并设置配置
        Node node = DefaultNode.getInstance();
        node.setConfig(config);

        // 初始化节点
        node.init();

        // 注册关闭钩子，确保优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (node) {
                node.notifyAll();
            }
        }));

        log.info("gracefully wait");

        // 等待关闭信号
        synchronized (node) {
            node.wait();
        }

        log.info("gracefully stop");
        // 销毁节点
        node.destroy();
    }

}
