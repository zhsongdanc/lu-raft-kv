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
import cn.think.in.java.LogModule;
import cn.think.in.java.Node;
import cn.think.in.java.StateMachine;
import cn.think.in.java.common.NodeConfig;
import cn.think.in.java.common.NodeStatus;
import cn.think.in.java.common.Peer;
import cn.think.in.java.common.PeerSet;
import cn.think.in.java.current.RaftThreadPool;
import cn.think.in.java.entity.*;
import cn.think.in.java.exception.RaftRemotingException;
import cn.think.in.java.membership.changes.ClusterMembershipChanges;
import cn.think.in.java.membership.changes.Result;
import cn.think.in.java.rpc.*;
import cn.think.in.java.util.LongConvert;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import raft.client.ClientKVAck;
import raft.client.ClientKVReq;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.think.in.java.common.NodeStatus.FOLLOWER;
import static cn.think.in.java.common.NodeStatus.LEADER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 抽象机器节点, 初始为 follower, 角色随时变化.
 *
 * @author 莫那·鲁道
 */
@Getter
@Setter
@Slf4j
public class DefaultNode implements Node, ClusterMembershipChanges {

    /** 选举超时时间基数（毫秒），实际选举时间会在此基础上增加随机值以避免冲突 */
    public volatile long electionTime = 15 * 1000;
    /** 上一次选举时间戳，用于判断是否到了选举时间 */
    public volatile long preElectionTime = 0;

    /** 上一次心跳时间戳，用于判断是否到了发送心跳的时间 */
    public volatile long preHeartBeatTime = 0;
    /** 心跳间隔时间（毫秒），Leader定期发送心跳维持地位 */
    public final long heartBeatTick = 5 * 100;


    /** 心跳任务，Leader定期发送心跳维持地位 */
    private HeartBeatTask heartBeatTask = new HeartBeatTask();
    /** 选举任务，Follower超时后发起选举 */
    private ElectionTask electionTask = new ElectionTask();
    /** 复制失败队列消费者，处理日志复制失败的重试逻辑 */
    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    /** 复制失败队列，存储需要重试的日志复制任务 */
    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);


    /**
     * 节点当前状态：FOLLOWER(0), CANDIDATE(1), LEADER(2)
     *
     * @see NodeStatus
     */
    public volatile int status = NodeStatus.FOLLOWER;

    /** 集群中所有节点的集合，包括自己 */
    public PeerSet peerSet;

    /** 节点运行状态标志 */
    volatile boolean running = false;

    /* ============ 所有服务器上持久存在的状态（需要持久化到磁盘） ============= */

    /** 服务器最后一次知道的任期号（初始化为 0，持续递增） */
    volatile long currentTerm = 0;
    /** 在当前任期获得选票的候选人的地址 */
    volatile String votedFor;
    /** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 */
    LogModule logModule;



    /* ============ 所有服务器上经常变的状态（易变状态） ============= */

    /** 已知的最大的已经被提交的日志条目的索引值 */
    volatile long commitIndex;

    /** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增) */
    volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的状态（选举后重新初始化） ================== */

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） */
    Map<Peer, Long> nextIndexs;

    /** 对于每一个服务器，已经复制给他的日志的最高索引值 */
    Map<Peer, Long> matchIndexs;



    /* ============================== 其他组件 ============================== */

    /** 节点配置信息 */
    public NodeConfig config;

    /** RPC服务端，处理来自其他节点的请求 */
    public RpcService rpcServer;

    /** RPC客户端，向其他节点发送请求 */
    public RpcClient rpcClient = new DefaultRpcClient();

    /** 状态机，存储实际的键值对数据 */
    public StateMachine stateMachine;

    /* ============================== 委托组件 ============================== */

    /** 一致性模块实现，处理Raft协议的核心逻辑 */
    Consensus consensus;

    /** 集群成员变更委托，处理节点的添加和删除 */
    ClusterMembershipChanges delegate;


    /* ============================== */

    private DefaultNode() {
    }

    public static DefaultNode getInstance() {
        return DefaultNodeLazyHolder.INSTANCE;
    }


    private static class DefaultNodeLazyHolder {

        private static final DefaultNode INSTANCE = new DefaultNode();
    }

    @Override
    public void init() throws Throwable {
        running = true;
        // 初始化RPC服务端和客户端
        rpcServer.init();
        rpcClient.init();

        // 初始化一致性模块和成员变更模块
        consensus = new DefaultConsensus(this);
        delegate = new ClusterMembershipChangesImpl(this);

        // 启动定时任务
        RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);  // 心跳任务，每500ms检查一次
        RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500); // 选举任务，6秒后开始，每500ms检查一次
        RaftThreadPool.execute(replicationFailQueueConsumer); // 启动复制失败队列消费者

        // 从日志中恢复当前任期号
        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            currentTerm = logEntry.getTerm();
        }

        log.info("start success, selfId : {} ", peerSet.getSelf());
    }

    @Override
    public void setConfig(NodeConfig config) {
        this.config = config;
        // 根据配置创建状态机实例
        stateMachine = config.getStateMachineSaveType().getStateMachine();
        // 获取日志模块实例
        logModule = DefaultLogModule.getInstance();

        // 初始化节点集合
        peerSet = PeerSet.getInstance();
        for (String s : config.getPeerAddrs()) {
            Peer peer = new Peer(s);
            peerSet.addPeer(peer);
            // 找到自己对应的节点
            if (s.equals("localhost:" + config.getSelfPort())) {
                peerSet.setSelf(peer);
            }
        }

        // 创建RPC服务端，绑定到指定端口
        rpcServer = new DefaultRpcServiceImpl(config.selfPort, this);
    }


    @Override
    public RvoteResult handlerRequestVote(RvoteParam param) {
        log.warn("handlerRequestVote will be invoke, param info : {}", param);
        return consensus.requestVote(param);
    }

    @Override
    public AentryResult handlerAppendEntries(AentryParam param) {
        if (param.getEntries() != null) {
            log.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }

        return consensus.appendEntries(param);
    }


    @Override
    public ClientKVAck redirect(ClientKVReq request) {
        Request r = Request.builder()
                .obj(request)
                .url(peerSet.getLeader().getAddr())
                .cmd(Request.CLIENT_REQ).build();

        return rpcClient.send(r);
    }

    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     * 领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
     *
     * @param request
     * @return
     */
    @Override
    public synchronized ClientKVAck handlerClientRequest(ClientKVReq request) {

        log.warn("handlerClientRequest handler {} operation,  and key : [{}], value : [{}]",
                ClientKVReq.Type.value(request.getType()), request.getKey(), request.getValue());

        if (status != LEADER) {
            log.warn("I not am leader , only invoke redirect method, leader addr : {}, my addr : {}",
                    peerSet.getLeader(), peerSet.getSelf().getAddr());
            return redirect(request);
        }

        if (request.getType() == ClientKVReq.GET) {
            LogEntry logEntry = stateMachine.get(request.getKey());
            if (logEntry != null) {
                return new ClientKVAck(logEntry);
            }
            return new ClientKVAck(null);
        }

        LogEntry logEntry = LogEntry.builder()
                .command(Command.builder().
                        key(request.getKey()).
                        value(request.getValue()).
                        build())
                .term(currentTerm)
                .build();

        // 预提交到本地日志, TODO 预提交
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());

        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new ArrayList<>();

        int count = 0;
        //  复制到其他机器
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            count++;
            // 并行发起 RPC 复制.
            futureList.add(replication(peer, logEntry));
        }

        // TODO szh 这里的size能否正确设置，任务执行完后
        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        getRPCAppendResult(futureList, latch, resultList);

        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }

        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
        // 小于 2, 没有意义
        int median = 0;
        if (matchIndexList.size() >= 2) {
            Collections.sort(matchIndexList);
            median = matchIndexList.size() / 2;
        }
        Long N = matchIndexList.get(median);
        if (N > commitIndex) {
            LogEntry entry = logModule.read(N);
            if (entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
        }

        //  响应客户端(成功一半)
        if (success.get() >= (count / 2)) {
            // 更新
            commitIndex = logEntry.getIndex();
            //  应用到状态机
            getStateMachine().apply(logEntry);
            lastApplied = commitIndex;

            log.info("success apply local state machine,  logEntry info : {}", logEntry);
            // 返回成功.
            return ClientKVAck.ok();
        } else {
            // 回滚已经提交的日志.
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("fail apply local state  machine,  logEntry info : {}", logEntry);
            // TODO 不应用到状态机,但已经记录到日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
            // 这里应该返回错误, 因为没有成功复制过半机器.
            return ClientKVAck.fail();
        }
    }

    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            RaftThreadPool.execute(() -> {
                try {
                    resultList.add(future.get(3000, MILLISECONDS));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    resultList.add(false);
                } finally {
                    latch.countDown();
                }
            });
        }
    }


    /** 复制到其他机器 */
    public Future<Boolean> replication(Peer peer, LogEntry entry) {

        return RaftThreadPool.submit(() -> {

            long start = System.currentTimeMillis(), end = start;

            // 20 秒重试时间
            while (end - start < 20 * 1000L) {

                AentryParam aentryParam = AentryParam.builder().build();
                aentryParam.setTerm(currentTerm);
                aentryParam.setServerId(peer.getAddr());
                aentryParam.setLeaderId(peerSet.getSelf().getAddr());

                aentryParam.setLeaderCommit(commitIndex);

                // 以我这边为准, 这个行为通常是成为 leader 后,首次进行 RPC 才有意义.
                // TODO szh
                Long nextIndex = nextIndexs.get(peer);
                LinkedList<LogEntry> logEntries = new LinkedList<>();
                if (entry.getIndex() >= nextIndex) {
                    for (long i = nextIndex; i <= entry.getIndex(); i++) {
                        LogEntry l = logModule.read(i);
                        if (l != null) {
                            logEntries.add(l);
                        }
                    }
                } else {
                    logEntries.add(entry);
                }
                // 最小的那个日志.
                LogEntry preLog = getPreLog(logEntries.getFirst());
                aentryParam.setPreLogTerm(preLog.getTerm());
                aentryParam.setPrevLogIndex(preLog.getIndex());

                aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));

                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(aentryParam)
                        .url(peer.getAddr())
                        .build();

                try {
                    AentryResult result = getRpcClient().send(request);
                    if (result == null) {
                        return false;
                    }
                    if (result.isSuccess()) {
                        log.info("append follower entry success , follower=[{}], entry=[{}]", peer, aentryParam.getEntries());
                        // update 这两个追踪值
                        nextIndexs.put(peer, entry.getIndex() + 1);
                        matchIndexs.put(peer, entry.getIndex());
                        return true;
                    } else if (!result.isSuccess()) {
                        // 对方比我大
                        if (result.getTerm() > currentTerm) {
                            log.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower",
                                    peer, result.getTerm(), currentTerm);
                            currentTerm = result.getTerm();
                            // 认怂, 变成跟随者
                            status = NodeStatus.FOLLOWER;
                            return false;
                        } // 没我大, 却失败了,说明 index 不对.或者 term 不对.
                        else {
                            // 递减
                            if (nextIndex == 0) {
                                nextIndex = 1L;
                            }
                            nextIndexs.put(peer, nextIndex - 1);
                            log.warn("follower {} nextIndex not match, will reduce nextIndex and retry RPC append, nextIndex : [{}]", peer.getAddr(),
                                    nextIndex);
                            // 重来, 直到成功.
                        }
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                    // TODO 到底要不要放队列重试?
//                        ReplicationFailModel model =  ReplicationFailModel.newBuilder()
//                            .callable(this)
//                            .logEntry(entry)
//                            .peer(peer)
//                            .offerTime(System.currentTimeMillis())
//                            .build();
//                        replicationFailQueue.offer(model);
                    return false;
                }
            }
            // 超时了,没办法了
            return false;
        });

    }

    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            log.warn("get perLog is null , parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(0L).term(0).command(null).build();
        }
        return entry;
    }


    class ReplicationFailQueueConsumer implements Runnable {

        /** 一分钟 */
        long intervalTime = 1000 * 60;

        @Override
        public void run() {
            while (running) {
                try {
                    ReplicationFailModel model = replicationFailQueue.poll(1000, MILLISECONDS);
                    if (model == null) {
                        continue;
                    }
                    if (status != LEADER) {
                        // 应该清空?
                        // TODO szh
                        replicationFailQueue.clear();
                        continue;
                    }
                    log.warn("replication Fail Queue Consumer take a task, will be retry replication, content detail : [{}]", model.logEntry);
                    long offerTime = model.offerTime;
                    if (System.currentTimeMillis() - offerTime > intervalTime) {
                        log.warn("replication Fail event Queue maybe full or handler slow");
                    }

                    // TODO szh 这里根本没有set，不理解
                    Callable<Boolean> callable = model.callable;
                    Future<Boolean> future = RaftThreadPool.submit(callable);
                    Boolean r = future.get(3000, MILLISECONDS);
                    // 重试成功.
                    if (r) {
                        // 可能有资格应用到状态机.
                        tryApplyStateMachine(model);
                    }

                } catch (InterruptedException e) {
                    // ignore
                } catch (ExecutionException | TimeoutException e) {
                    log.warn(e.getMessage());
                }
            }
        }
    }

    private void tryApplyStateMachine(ReplicationFailModel model) {

        String success = stateMachine.getString(model.successKey);
        stateMachine.setString(model.successKey, String.valueOf(Integer.parseInt(success) + 1));

        String count = stateMachine.getString(model.countKey);

        if (Integer.parseInt(success) >= Integer.parseInt(count) / 2) {
            stateMachine.apply(model.logEntry);
            stateMachine.delString(model.countKey, model.successKey);
        }
    }


    @Override
    public void destroy() throws Throwable {
        rpcServer.destroy();
        stateMachine.destroy();
        rpcClient.destroy();
        running = false;
        log.info("destroy success");
    }


    /**
     * 选举任务：实现Raft协议的Leader选举逻辑
     * 
     * 选举流程：
     * 1. 在转变成候选人后就立即开始选举过程
     * 2. 自增当前的任期号（currentTerm）
     * 3. 给自己投票
     * 4. 重置选举超时计时器
     * 5. 发送请求投票的 RPC 给其他所有服务器
     * 6. 如果接收到大多数服务器的选票，那么就变成领导人
     * 7. 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
     * 8. 如果选举过程超时，再次发起一轮选举
     */
    class ElectionTask implements Runnable {

        @Override
        public void run() {
            // 如果已经是Leader，不需要选举
            if (status == LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            // 基于 RAFT 的随机时间，解决选举冲突
            electionTime = electionTime + ThreadLocalRandom.current().nextInt(50);
            // 检查是否到了选举时间
            if (current - preElectionTime < electionTime) {
                return;
            }
            
            // 转为候选人状态，开始选举
            status = NodeStatus.CANDIDATE;
            log.error("node {} will become CANDIDATE and start election leader, current term : [{}], LastEntry : [{}]",
                    peerSet.getSelf(), currentTerm, logModule.getLast());

            // 设置下次选举时间，增加随机性避免冲突
            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;

            // 自增任期号
            currentTerm = currentTerm + 1;
            // 给自己投票
            votedFor = peerSet.getSelf().getAddr();

            // 获取除自己外的所有节点
            List<Peer> peers = peerSet.getPeersWithOutSelf();
            ArrayList<Future<RvoteResult>> futureArrayList = new ArrayList<>();

            log.info("peerList size : {}, peer list content : {}", peers.size(), peers);

            // 向所有其他节点发送投票请求
            for (Peer peer : peers) {
                futureArrayList.add(RaftThreadPool.submit(() -> {
                    // 获取自己最后一条日志的任期号
                    long lastTerm = 0L;
                    LogEntry last = logModule.getLast();
                    if (last != null) {
                        lastTerm = last.getTerm();
                    }

                    // 构建投票请求参数
                    RvoteParam param = RvoteParam.builder().
                            term(currentTerm).                    // 当前任期号
                            candidateId(peerSet.getSelf().getAddr()). // 候选人ID（自己）
                            lastLogIndex(LongConvert.convert(logModule.getLastIndex())). // 最后日志索引
                            lastLogTerm(lastTerm).               // 最后日志任期号
                            build();

                    // 构建RPC请求
                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)  // 请求投票命令
                            .obj(param)
                            .url(peer.getAddr())
                            .build();

                    try {
                        // 发送投票请求
                        return getRpcClient().<RvoteResult>send(request);
                    } catch (RaftRemotingException e) {
                        log.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                        return null;
                    }
                }));
            }

            AtomicInteger success2 = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());

            log.info("futureArrayList.size() : {}", futureArrayList.size());
            // 等待结果.
            for (Future<RvoteResult> future : futureArrayList) {
                RaftThreadPool.submit(() -> {
                    try {
                        RvoteResult result = future.get(3000, MILLISECONDS);
                        if (result == null) {
                            return -1;
                        }
                        // TODO 是否一定会响应呢？
                        boolean isVoteGranted = result.isVoteGranted();

                        if (isVoteGranted) {
                            success2.incrementAndGet();
                        } else {
                            // 更新自己的任期.
                            long resTerm = result.getTerm();
                            if (resTerm >= currentTerm) {
                                currentTerm = resTerm;
                            }
                        }
                        return 0;
                    } catch (Exception e) {
                        log.error("future.get exception , e : ", e);
                        return -1;
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                // 稍等片刻
                latch.await(3500, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }

            int success = success2.get();
            log.info("node {} maybe become leader , success count = {} , status : {}", peerSet.getSelf(), success, NodeStatus.Enum.value(status));
            // 如果投票期间,有其他服务器发送 appendEntry , 就可能变成 follower ,这时,应该停止.
            if (status == NodeStatus.FOLLOWER) {
                return;
            }
            // 加上自身.
            if (success >= peers.size() / 2) {
                log.warn("node {} become leader ", peerSet.getSelf());
                status = LEADER;
                peerSet.setLeader(peerSet.getSelf());
                votedFor = "";
                becomeLeaderToDoThing();
            } else {
                // else 重新选举
                votedFor = "";
            }
            // 再次更新选举时间 TODO ??
            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;

        }
    }

    /**
     * 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1. 如果下次 RPC 时, 跟随者和leader 不一致,就会失败.
     * 那么 leader 尝试递减 nextIndex 并进行重试.最终将达成一致.
     * // TODO 这里是否有可能还有没提交的？，猜测有可能，但是这里好像不是这么做的
     */
    private void becomeLeaderToDoThing() {
        nextIndexs = new ConcurrentHashMap<>();
        matchIndexs = new ConcurrentHashMap<>();
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
            matchIndexs.put(peer, 0L);
        }

        // 创建[空日志]并提交，用于处理前任领导者未提交的日志
        LogEntry logEntry = LogEntry.builder()
                .command(null)
                .term(currentTerm)
                .build();

        // 预提交到本地日志, TODO 预提交
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());

        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new ArrayList<>();

        int count = 0;
        //  复制到其他机器
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            // TODO szh能这样写吗
            count++;
            // 并行发起 RPC 复制.
            futureList.add(replication(peer, logEntry));
        }

        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        getRPCAppendResult(futureList, latch, resultList);

        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }

        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
        // 小于 2, 没有意义
        int median = 0;
        if (matchIndexList.size() >= 2) {
            Collections.sort(matchIndexList);
            median = matchIndexList.size() / 2;
        }
        Long N = matchIndexList.get(median);
        if (N > commitIndex) {
            LogEntry entry = logModule.read(N);
            if (entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
        }

        //  响应客户端(成功一半)
        if (success.get() >= (count / 2)) {
            // 更新
            commitIndex = logEntry.getIndex();
            //  应用到状态机
            getStateMachine().apply(logEntry);
            lastApplied = commitIndex;

            log.info("success apply local state machine,  logEntry info : {}", logEntry);
        } else {
            // 回滚已经提交的日志
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("fail apply local state  machine,  logEntry info : {}", logEntry);

            // 无法提交空日志，让出领导者位置
            log.warn("node {} becomeLeaderToDoThing fail ", peerSet.getSelf());
            status = FOLLOWER;
            peerSet.setLeader(null);
            votedFor = "";
        }

    }


    /**
     * 心跳任务：Leader定期发送心跳维持地位
     * 
     * 心跳的作用：
     * 1. 维持Leader地位，防止其他节点发起选举
     * 2. 同步commitIndex，让Follower知道哪些日志已提交
     * 3. 检测是否有更高任期的Leader出现
     */
    class HeartBeatTask implements Runnable {

        @Override
        public void run() {
            // 只有Leader才发送心跳
            if (status != LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            // 检查是否到了发送心跳的时间
            if (current - preHeartBeatTime < heartBeatTick) {
                return;
            }
            
            // 打印每个Follower的nextIndex信息，用于调试
            log.info("=========== NextIndex =============");
            for (Peer peer : peerSet.getPeersWithOutSelf()) {
                log.info("Peer {} nextIndex={}", peer.getAddr(), nextIndexs.get(peer));
            }

            preHeartBeatTime = System.currentTimeMillis();

            // 向所有Follower发送心跳
            for (Peer peer : peerSet.getPeersWithOutSelf()) {
                // 构建心跳参数（空日志）
                AentryParam param = AentryParam.builder()
                        .entries(null)  // 心跳，空日志
                        .leaderId(peerSet.getSelf().getAddr())  // Leader ID
                        .serverId(peer.getAddr())  // 目标节点ID
                        .term(currentTerm)  // 当前任期号
                        .leaderCommit(commitIndex) // 心跳时与跟随者同步 commit index
                        .build();

                Request request = new Request(
                        Request.A_ENTRIES,  // 附加日志命令
                        param,
                        peer.getAddr());

                // 异步发送心跳
                RaftThreadPool.execute(() -> {
                    try {
                        AentryResult aentryResult = getRpcClient().send(request);
                        long term = aentryResult.getTerm();

                        // 如果发现更高任期的节点，转为Follower
                        if (term > currentTerm) {
                            log.error("self will become follower, he's term : {}, my term : {}", term, currentTerm);
                            currentTerm = term;
                            votedFor = "";
                            status = NodeStatus.FOLLOWER;
                        }
                    } catch (Exception e) {
                        log.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                    }
                }, false);
            }
        }
    }

    @Override
    public Result addPeer(Peer newPeer) {
        return delegate.addPeer(newPeer);
    }

    @Override
    public Result removePeer(Peer oldPeer) {
        return delegate.removePeer(oldPeer);
    }

}
