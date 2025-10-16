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
package cn.think.in.java.entity;

import cn.think.in.java.LogModule;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 日志条目 - Raft协议中的核心数据结构
 * 
 * 每个日志条目包含：
 * 1. index：日志条目的索引号，从1开始递增
 * 2. term：创建该日志条目时的任期号
 * 3. command：用户状态机要执行的命令
 *
 * @author 莫那·鲁道
 * @see LogModule
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEntry implements Serializable, Comparable<LogEntry> {

    /** 日志条目的索引号，从1开始递增 */
    private Long index;

    /** 创建该日志条目时的任期号 */
    private long term;

    /** 用户状态机要执行的命令 */
    private Command command;

    /**
     * 比较两个日志条目的索引号
     * 用于日志条目的排序和比较
     * 
     * @param o 要比较的另一个日志条目
     * @return 比较结果：-1表示小于，0表示等于，1表示大于
     */
    @Override
    public int compareTo(LogEntry o) {
        if (o == null) {
            return -1;
        }
        if (this.getIndex() > o.getIndex()) {
            return 1;
        }
        return -1;
    }



}
