package rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 简易版的，当然系简单来搞,这里没有考虑什么并发
 */
public class SimpleRpc {

    //通讯的请求
    private static Map<String, List<String>> dataMap = new HashMap<>();

    /**
     * 发送请求
     *
     * @param who
     * @param data
     */
    public static void sendResponse(String who, String data) {
        System.out.println("消息发送给:" + who + ",内容:" + data);
        if (dataMap.containsKey(who)) {
            List<String> list = dataMap.get(who);
            list.add(data);
            dataMap.put(who, list);
        } else {
            List<String> list = new ArrayList<>();
            list.add(data);
            dataMap.put(who, list);
        }
    }

    /**
     * 获取信息
     *
     * @param who
     * @return
     */
    public static List<String> getResponse(String who) {
        List<String> list = dataMap.get(who);

        //清空数据
        dataMap.put(who, new ArrayList<>());

        return list;
    }


}
