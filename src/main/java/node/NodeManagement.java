package node;

import com.alibaba.fastjson.JSON;
import consistency.RaftConsistencyHandler;
import rpc.SimpleRpc;
import timer.TimerModule;
import timer.ValidityOrHeartbeatTimer;
import timer.bo.HeartBeatPack;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 节点管理器
 */
public class NodeManagement {

    //所有节点列表
    private List<Node> allNodeList;

    //主节点
    public Node leader;

    //一致性模块
    private RaftConsistencyHandler consistencyHandler;

    public NodeManagement(List<Node> allNodeList) {
        this.allNodeList = allNodeList;
        this.consistencyHandler = new RaftConsistencyHandler();
    }

    public RaftConsistencyHandler getConsistencyHandler() {
        return consistencyHandler;
    }

    public void pushNodeToLeader(String serviceId) {
        //这里没做异常处理
        Node node = allNodeList.stream().filter(item -> item.getServiceId().equals(serviceId)).findFirst().get();
        node.setLeader(true);

        leader = node;

        //默认2秒
        node.setHeartBeatTimer(new TimerModule().build(new ValidityOrHeartbeatTimer(Node.HEART_BEAT_TIME_OUT) {
            @Override
            public void trigger() {
                for (Node followerNode : getFollowerList(node.getServiceId())) {
                    //发送心跳
                    SimpleRpc.sendResponse(followerNode.getServiceId(), "h" + JSON.toJSONString(HeartBeatPack
                            .builder()
                            .serviceId(node.getServiceId())
                            .term(node.getTerm().get())
                            .lastLogIndex(node.getLogMachine().getLastIndex())
                            .lastCommitIndex(node.getStateMachine().getLastIndex())
                            .build()));
                }

                //又是新的一轮
                node.getHeartBeatTimer().resetHandler(false);
            }
        }));

        //快速发个心跳先
        node.getHeartBeatTimer().addTimeoutTaskRunNow();

        node.getHeartBeatTimer().addTimeoutTask();
    }

    public Node getNodeByServiceId(String serviceId) {
        //这里没做异常处理
        return allNodeList.stream().filter(item -> item.getServiceId().equals(serviceId)).findFirst().get();
    }

    public boolean checkNodeIsLeader(String serviceId) {
        //这里没做异常处理
        return leader.getServiceId().equals(serviceId);
    }

    public List<Node> getAllList() {
        return allNodeList;
    }

    public List<Node> getFollowerList(String serviceId) {
        return allNodeList.stream().filter(item -> !item.getServiceId().equals(serviceId)).collect(Collectors.toList());
    }

    public void addNode(Node node) {
        //忽略去重，因为简易版
        allNodeList.add(node);
    }

    public void deleteNode(Node node) {
        //忽略去重，因为简易版
        allNodeList.removeIf(item -> item.equals(node));
    }

}
