package node;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import fsm.StateMachine;
import log.LogDataBO;
import log.LogMachine;
import lombok.Data;
import rpc.SimpleRpc;
import timer.TimerModule;
import timer.ValidityOrHeartbeatTimer;
import timer.bo.ElementPack;
import timer.bo.HeartBeatPack;
import timer.bo.VotePack;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class Node {

    //节点控制只同步一次
    private AtomicInteger synCount = new AtomicInteger(1);

    //选举超时时间
    public static final int ELECTIONS_TIME_OUT = 5000;

    //心跳超时时间
    public static final int HEART_BEAT_TIME_OUT = 1000;

    //唯一标识，这里就没有id：port,简易版要什么自行车
    private String serviceId;

    //当前的任期，主要是图个方便，自增
    private AtomicInteger term = new AtomicInteger(0);

    //默认不是主节点
    private boolean isLeader = false;

    //节点角色，默认是跟随者
    private NodeRoleEnum role = NodeRoleEnum.FOLLOWER;

    //状态机
    private StateMachine stateMachine;

    //日志记录
    private LogMachine logMachine;

    // 选举定时器
    private TimerModule electionsTimer;

    // leader节点心跳定时器
    private TimerModule heartBeatTimer;

    //节点管理器
    private NodeManagement nodeManagement;

    //节点是否开启
    private boolean isStart = false;

    public Node(String serviceId) {
        this.serviceId = serviceId;
        this.stateMachine = new StateMachine(serviceId);
        this.logMachine = new LogMachine(serviceId);
        //默认5秒
        this.electionsTimer = new TimerModule().build(new ValidityOrHeartbeatTimer(ELECTIONS_TIME_OUT) {
            @Override
            public void trigger() {
                timeout();

                //又是新的一轮
                electionsTimer.resetHandler(false);
            }
        });
    }

    /**
     * 节点超时的操作
     */
    private void timeout() {
        //如果是主节点发送心跳
        if (isLeader) {
            //需要进行心跳
            List<Node> followerList = nodeManagement.getFollowerList(serviceId);
            followerList.forEach(item -> {
                HeartBeatPack pack = HeartBeatPack
                        .builder()
                        .serviceId(serviceId)
                        .term(term.get())
                        .lastLogIndex(logMachine.getLastIndex())
                        .build();
                //心跳包发送
                SimpleRpc.sendResponse(item.getServiceId(), "v" + JSON.toJSONString(pack));
            });

            return;
        }

        nodeManagement.leader = null;

        //如果是跟随者则发出投票
        role = NodeRoleEnum.CANDIDATE;

        term.addAndGet(1);

        //先投给自己，这个老六
        nodeManagement.getConsistencyHandler().voteToTho(serviceId, term.get());

        List<Node> allList = nodeManagement.getAllList();
        allList.stream().filter(item -> !item.getServiceId().equals(serviceId)).forEach(item -> {
            VotePack pack = VotePack
                    .builder()
                    .serviceId(serviceId)
                    .term(term.get())
                    .lastLogIndex(logMachine.getLastIndex())
                    .build();
            //投票包发送
            SimpleRpc.sendResponse(item.getServiceId(), "v" + JSON.toJSONString(pack));
        });
    }

    public void setNodeList(List<Node> allNodeList) {
        this.nodeManagement = new NodeManagement(allNodeList);
    }

    /**
     * 处理请求
     */
    private void handlerRequest(List<String> requestList) {
        requestList.forEach(request -> {
            //消息类型
            String type = request.substring(0, 1);
            String msg = request.substring(1);

            HeartBeatPack heartBeatPack = null;
            VotePack votePack = null;
            ElementPack elementPack = null;

            switch (type) {
                case "v":
                    votePack = JSON.parseObject(msg, VotePack.class);
                    break;
                case "h":
                    heartBeatPack = JSON.parseObject(msg, HeartBeatPack.class);
                    break;
                case "e":
                    elementPack = JSON.parseObject(msg, ElementPack.class);
                    break;
                default:
                    break;
            }

            if (heartBeatPack != null) {
                //处理心跳
                processHeartbeat(heartBeatPack);
            }

            if (votePack != null) {
                //处理投票
                processVote(votePack);
            }

            if (elementPack != null) {
                //处理数据变更
                processElement(elementPack);
            }

        });

    }

    /**
     * 提交东西
     */
    public void addSomeThing(String data) {
        //判断是否是主节点，不是的话需要转发
        if (isLeader) {
            logMachine.addLog(LogDataBO
                    .builder()
                    .term((long) term.get())
                    .data(data)
                    .build());

            for (Node node : nodeManagement.getFollowerList(serviceId)) {
                System.out.println("给跟随者发送信息：" + node.serviceId + "，数据：" + data);

                SimpleRpc.sendResponse(node.getServiceId(), "e" + JSON.toJSONString(ElementPack
                        .builder()
                        .serviceId(serviceId)
                        .lastLogIndex(logMachine.getLastIndex())
                        .term(term.get())
                        .data(data)
                        .build()));
            }
        } else {
            String leaderServiceId = nodeManagement.leader.getServiceId();

            if (StrUtil.isBlank(leaderServiceId)) {
                nodeManagement.leader = null;
                //随便丢给别人解析
                leaderServiceId = nodeManagement.getFollowerList(serviceId).get(0).getServiceId();
            }

            SimpleRpc.sendResponse(leaderServiceId, "e" + JSON.toJSONString(ElementPack
                    .builder()
                    .forward(true)
                    .serviceId(serviceId)
                    .term(term.get())
                    .lastLogIndex(logMachine.getLastIndex())
                    .data(data)
                    .build()));
        }
    }

    /**
     * 处理心跳
     */
    private void processHeartbeat(HeartBeatPack heartBeatPack) {
        if (heartBeatPack.getTerm() < term.get()) {
            return;
        }

        term = new AtomicInteger(heartBeatPack.getTerm());

        role = NodeRoleEnum.FOLLOWER;

        //设置leader
        nodeManagement.leader = nodeManagement.getNodeByServiceId(heartBeatPack.getServiceId());

        //处理日志同步
        if (heartBeatPack.getLastLogIndex() != logMachine.getLastIndex()) {

            CompletableFuture.runAsync(() -> {
                //控制只同步一次
                if (synCount.get() != 1) {
                    return;
                }

                synCount.decrementAndGet();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //需要leader给数据
                ElementPack.ElementPackResponse response = ElementPack.ElementPackResponse
                        .builder()
                        .result(false)
                        .build();

                SimpleRpc.sendResponse(heartBeatPack.getServiceId(), "e" + JSON.toJSONString(ElementPack.builder().serviceId(serviceId).response(response).build()));

                synCount.addAndGet(1);
            });
        }

        if (heartBeatPack.getLastCommitIndex() > stateMachine.getLastIndex()) {
            for (long i = stateMachine.getLastIndex() + 1; i <= heartBeatPack.getLastCommitIndex(); i++) {
                //刷到状态机
                stateMachine.apply(logMachine.getLog((int) i));
            }
        }

        //续期
        electionsTimer.resetHandler(true);
    }

    /**
     * 处理投票
     */
    private void processVote(VotePack votePack) {
        //处理返回结果
        if (votePack.getResponse() != null) {
            if (votePack.getResponse().isVoteResult()) {
                nodeManagement.getConsistencyHandler().voteToTho(votePack.getServiceId(), term.get());
            }

            boolean isGrantedToLeader = nodeManagement.getConsistencyHandler().isGrantedToLeader(votePack.getTerm(), nodeManagement.getAllList().size());
            if (isGrantedToLeader) {
                boolean isGranted = grantToLeader();

                if (isGranted) {
                    isLeader = true;
                    System.out.println("节点:" + serviceId + "在任期:" + term + "已经成为leader");
                } else {
                    System.out.println("节点:" + serviceId + "在任期:" + term + "授权leader失败");
                }
            }

            return;
        }

        //对比任期、最大的日志index大小
        if (term.get() >= votePack.getTerm()) {
            return;
        }

        //修改任期
        term = new AtomicInteger(votePack.getTerm());

        if (isLeader) {
            //别人任期都比你大了
            setHeartBeatTimer(null);
            role = NodeRoleEnum.FOLLOWER;
            isLeader = false;
            nodeManagement.leader = null;
        }

        if (logMachine.getLastIndex() > votePack.getLastLogIndex()) {
            return;
        }

        //回复结果
        SimpleRpc.sendResponse(votePack.getServiceId(), "v" + JSON.toJSONString(VotePack
                .builder()
                .serviceId(serviceId)
                .term(votePack.getTerm())
                .response(VotePack.VotePackResponse.builder().voteResult(true).build())
                .build()));
    }

    /**
     * 处理数据变更
     */
    private void processElement(ElementPack elementPack) {
        ElementPack.ElementPackResponse elementPackResponse = elementPack.getResponse();
        if (elementPackResponse != null) {
            if (elementPackResponse.getResult()) {
                //提交日志同步结果
                int logIndex = (int) elementPack.getLastLogIndex();

                logMachine.confirmLog(logIndex, elementPack.getServiceId(), nodeManagement.getAllList());

                List<LogMachine.LogConfirm> confirmList = logMachine.getConfirmResultByIndex(logIndex);

                if ((confirmList.size() / 2 + 1) < confirmList.stream().filter(item -> item.getConfirmed().equals(true)).count()) {
                    //提交状态机
                    stateMachine.apply(logMachine.getLog(logIndex));
                }
            } else {
                //心跳的时候，发现数据对不上，需要回传所有数据
                List<LogDataBO> dataList = elementPackResponse.getDataList();

                if (CollectionUtil.isEmpty(dataList)) {
                    return;
                }

                List<LogDataBO> leaderDataList = logMachine.getAllLog();

                ElementPack pack = ElementPack
                        .builder()
                        .serviceId(serviceId)
                        .leaderDataList(leaderDataList)
                        .lastCommitIndex(stateMachine.getLastIndex())
                        .build();

                //回传follower节点自行对照
                SimpleRpc.sendResponse(elementPack.getServiceId(), "e" + JSON.toJSONString(pack));
            }

            return;
        }

        List<LogDataBO> leaderList = elementPack.getLeaderDataList();

        /**
         * 强行覆盖所有日志
         */
        if (CollectionUtil.isNotEmpty(leaderList)) {
            this.stateMachine = new StateMachine(serviceId);
            this.logMachine = new LogMachine(serviceId);

            logMachine.resetAllLog(leaderList);
            stateMachine.resetAllLog(leaderList);
            return;
        }

        //对比任期、最大的日志index大小
        if (term.get() != elementPack.getTerm()) {
            return;
        }

        //等心跳再同步数据先，再插入
        if (elementPack.isForward()) {
            if (logMachine.getLastIndex() > elementPack.getLastLogIndex()) {
                return;
            }
        }else {
            if (logMachine.getLastIndex() >= elementPack.getLastLogIndex()) {
                return;
            }
        }

        logMachine.addLog(LogDataBO
                .builder()
                .term((long) elementPack.getTerm())
                .data(elementPack.getData())
                .build());

        if (elementPack.isForward()) {
            for (Node node : nodeManagement.getFollowerList(serviceId)) {
                System.out.println("给跟随者发送信息：" + node.serviceId + "，数据：" + elementPack.getData());

                SimpleRpc.sendResponse(node.getServiceId(), "e" + JSON.toJSONString(ElementPack
                        .builder()
                        .term(term.get())
                        .serviceId(serviceId)
                        .lastLogIndex(logMachine.getLastIndex())
                        .data(elementPack.getData())
                        .build()));
            }

            return;
        }

        //具体应用到状态机的话，等心跳确认/
        ElementPack.ElementPackResponse response = ElementPack.ElementPackResponse
                .builder()
                .result(true)
                .build();

        ElementPack pack = ElementPack.builder().serviceId(serviceId).lastLogIndex(logMachine.getLastIndex()).response(response).build();

        SimpleRpc.sendResponse(elementPack.getServiceId(), "e" + JSON.toJSONString(pack));
    }

    /**
     * 启动节点
     */
    public void start() {
        isStart = true;

        System.out.println("节点：" + serviceId + "启动......");
        electionsTimer.addTimeoutTask();

        //定时拿数据
        CompletableFuture.runAsync(() -> {
            while (isStart) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                List<String> requestList = SimpleRpc.getResponse(serviceId);

                if (CollectionUtil.isEmpty(requestList)) {
                    continue;
                }

                System.out.println("节点:" + serviceId + "收到信息:" + requestList);
//                System.out.println("节点:" + serviceId + "自身node信息:" + this);

                handlerRequest(requestList);
            }
        });
    }

    /**
     * 关闭节点
     */
    public void stop() {
        isStart = false;
        //todo
    }

    /**
     * 被授权为主节点
     */
    public boolean grantToLeader() {
        if (role != NodeRoleEnum.CANDIDATE) {
            return false;
        }

        nodeManagement.pushNodeToLeader(serviceId);

        return true;
    }

    /**
     * 节点角色
     */
    public enum NodeRoleEnum {

        LEADER,
        FOLLOWER,
        CANDIDATE;

    }

}
