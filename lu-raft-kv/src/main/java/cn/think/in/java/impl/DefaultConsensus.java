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
package cn.think.in.java.impl;

import cn.think.in.java.Consensus;
import cn.think.in.java.common.NodeStatus;
import cn.think.in.java.common.Peer;
import cn.think.in.java.entity.*;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 默认的一致性模块实现
 * 
 * 实现了Raft协议的核心一致性逻辑：
 * 1. 请求投票RPC - 处理来自候选人的投票请求
 * 2. 附加日志RPC - 处理来自Leader的日志复制请求
 *
 * @author 莫那·鲁道
 */
@Setter
@Getter
public class DefaultConsensus implements Consensus {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);

    /** 关联的节点实例 */
    public final DefaultNode node;

    /** 投票锁，确保投票请求的原子性处理 */
    public final ReentrantLock voteLock = new ReentrantLock();
    /** 附加日志锁，确保日志复制请求的原子性处理 */
    public final ReentrantLock appendLock = new ReentrantLock();

    public DefaultConsensus(DefaultNode node) {
        this.node = node;
    }

    /**
     * 请求投票 RPC - 处理来自候选人的投票请求
     *
     * Raft协议投票规则：
     * 1. 如果term < currentTerm返回 false （5.2 节）
     * 2. 如果 votedFor 为空或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
     * 
     * @param param 投票请求参数
     * @return 投票结果
     */
    @Override
    public RvoteResult requestVote(RvoteParam param) {
        try {
            RvoteResult.Builder builder = RvoteResult.newBuilder();
            // 尝试获取投票锁，如果获取失败说明有其他投票请求在处理
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 规则1：如果对方任期号小于自己的任期号，拒绝投票
            if (param.getTerm() < node.getCurrentTerm()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 记录投票信息，用于调试
            LOGGER.info("node {} current vote for [{}], param candidateId : {}", node.peerSet.getSelf(), node.getVotedFor(), param.getCandidateId());
            LOGGER.info("node {} current term {}, peer term : {}", node.peerSet.getSelf(), node.getCurrentTerm(), param.getTerm());

            // 规则2：检查是否可以投票给这个候选人
            if ((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {
                // 检查候选人的日志是否至少和自己一样新
                if (node.getLogModule().getLast() != null) {
                    // 如果候选人的最后日志任期号小于自己的，拒绝投票
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        return RvoteResult.fail();
                    }
                    // 如果候选人的最后日志索引小于自己的，拒绝投票
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        return RvoteResult.fail();
                    }
                }

                // 投票给候选人，更新节点状态
                node.status = NodeStatus.FOLLOWER;  // 转为Follower状态
                node.peerSet.setLeader(new Peer(param.getCandidateId()));  // 设置新的Leader
                node.setCurrentTerm(param.getTerm());  // 更新任期号
                node.setVotedFor(param.getCandidateId());  // 记录投票给谁
                // 返回投票成功
                return builder.term(node.currentTerm).voteGranted(true).build();
            }

            // 已经投票给其他人，拒绝投票
            return builder.term(node.currentTerm).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }


    /**
     * 附加日志 RPC - 处理来自Leader的日志复制请求
     *
     * Raft协议附加日志规则：
     * 1. 如果 term < currentTerm 就返回 false （5.1 节）
     * 2. 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
     * 3. 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
     * 4. 附加任何在已有的日志中不存在的条目
     * 5. 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     * 
     * @param param 附加日志请求参数
     * @return 附加日志结果
     */
    @Override
    public AentryResult appendEntries(AentryParam param) {
        AentryResult result = AentryResult.fail();
        try {
            // 尝试获取附加日志锁，确保原子性处理
            if (!appendLock.tryLock()) {
                return result;
            }

            result.setTerm(node.getCurrentTerm());
            // 规则1：如果Leader的任期号小于自己的任期号，拒绝请求
            if (param.getTerm() < node.getCurrentTerm()) {
                return result;
            }

            // 收到有效Leader的请求，重置选举和心跳时间
            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();
            node.peerSet.setLeader(new Peer(param.getLeaderId()));

            // 如果Leader的任期号大于等于自己的任期号，转为Follower
            if (param.getTerm() >= node.getCurrentTerm()) {
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId = {}",
                    node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                node.status = NodeStatus.FOLLOWER;
            }
            // 更新自己的任期号为Leader的任期号
            node.setCurrentTerm(param.getTerm());

            // 处理心跳请求（空日志）
            if (param.getEntries() == null || param.getEntries().length == 0) {
                LOGGER.info("node {} append heartbeat success , he's term : {}, my term : {}",
                    param.getLeaderId(), param.getTerm(), node.getCurrentTerm());

                // 处理Leader已提交但未应用到状态机的日志
                // 下一个需要提交的日志的索引
                long nextCommit = node.getCommitIndex() + 1;

                // 规则5：如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
                if (param.getLeaderCommit() > node.getCommitIndex()) {
                    int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                    node.setCommitIndex(commitIndex);
                    node.setLastApplied(commitIndex);
                }

                // 应用所有已提交但未应用的日志到状态机
                while (nextCommit <= node.getCommitIndex()){
                    node.stateMachine.apply(node.logModule.read(nextCommit));
                    nextCommit++;
                }
                return AentryResult.newBuilder().term(node.getCurrentTerm()).success(true).build();
            }

            // 真实日志
            // 第一次
            if (node.getLogModule().getLastIndex() != 0 && param.getPrevLogIndex() != 0) {
                LogEntry logEntry;
                if ((logEntry = node.getLogModule().read(param.getPrevLogIndex())) != null) {
                    // 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
                    // 需要减小 nextIndex 重试.
                    if (logEntry.getTerm() != param.getPreLogTerm()) {
                        return result;
                    }
                } else {
                    // index 不对, 需要递减 nextIndex 重试.
                    return result;
                }

            }

            // TODO szh
            // 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
            LogEntry existLog = node.getLogModule().read(((param.getPrevLogIndex() + 1)));
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()) {
                // 删除这一条和之后所有的, 然后写入日志和状态机.
                node.getLogModule().removeOnStartIndex(param.getPrevLogIndex() + 1);
                // todo 上面只比较了一条，会不会出现问题？
            } else if (existLog != null) {
                // 已经有日志了, 不能重复写入.
                result.setSuccess(true);
                return result;
            }

            // 写进日志
            for (LogEntry entry : param.getEntries()) {
                node.getLogModule().write(entry);
                result.setSuccess(true);
            }

            // 下一个需要提交的日志的索引（如有）
            long nextCommit = node.getCommitIndex() + 1;

            //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
            if (param.getLeaderCommit() > node.getCommitIndex()) {
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                node.setCommitIndex(commitIndex);
                node.setLastApplied(commitIndex);
            }

            while (nextCommit <= node.getCommitIndex()){
                // 提交之前的日志
                node.stateMachine.apply(node.logModule.read(nextCommit));
                nextCommit++;
            }

            result.setTerm(node.getCurrentTerm());

            node.status = NodeStatus.FOLLOWER;
            // TODO, 是否应当在成功回复之后, 才正式提交? 防止 leader "等待回复"过程中 挂掉.
            return result;
        } finally {
            appendLock.unlock();
        }
    }


}
