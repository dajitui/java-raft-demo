import node.Node;

import java.util.ArrayList;
import java.util.List;

public class RaftTest {

    public static void main(String[] args) throws InterruptedException {
        //1、初始化node节点
        List<Node> nodeList = new ArrayList<>();

        Node one = new Node("one");
        Node two = new Node("two");
        Node three = new Node("three");

        nodeList.add(one);
        nodeList.add(two);
        nodeList.add(three);

        one.setNodeList(nodeList);
        two.setNodeList(nodeList);
        three.setNodeList(nodeList);

        //2、启动节点，开始跑起来
        nodeList.forEach(Node::start);

        //等待选举完
        Thread.sleep(10000);

        //3、选举出leader之后进行+1
        one.addSomeThing("123");

        //等待全部数据复制完
        Thread.sleep(5000);


        System.out.println("---日志---");
        System.out.println(one.getLogMachine().getLog(1));
        System.out.println(two.getLogMachine().getLog(1));
        System.out.println(three.getLogMachine().getLog(1));

        //4、查询随意node里面的值是否一致
        System.out.println("---状态机---");
        System.out.println(one.getStateMachine().getFsm().getElement(1));
        System.out.println(two.getStateMachine().getFsm().getElement(1));
        System.out.println(three.getStateMachine().getFsm().getElement(1));

        one.addSomeThing("666");

        //等待全部数据复制完
        Thread.sleep(5000);


        System.out.println("---日志---");
        System.out.println(one.getLogMachine().getLog(2));
        System.out.println(two.getLogMachine().getLog(2));
        System.out.println(three.getLogMachine().getLog(2));

        //4、查询随意node里面的值是否一致
        System.out.println("---状态机---");
        System.out.println(one.getStateMachine().getFsm().getElement(2));
        System.out.println(two.getStateMachine().getFsm().getElement(2));
        System.out.println(three.getStateMachine().getFsm().getElement(2));
    }

}
