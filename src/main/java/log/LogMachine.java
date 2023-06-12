package log;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import node.Node;
import rocksdb.RocksDbHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class LogMachine {

    //数据从节点落库情况
    Map<Integer, List<LogConfirm>> map = new HashMap<>();

    //日志实际处理器
    private static RocksDbHandler logMachine;

    public LogMachine(String serviceId) {
        //初始化状态机 rocksdb
        logMachine = new RocksDbHandler("LogMachine" + serviceId);
    }

    /**
     * 获取提交结果
     *
     * @return
     */
    public List<LogConfirm> getConfirmResultByIndex(Integer index) {
        return map.get(index);
    }

    /**
     * 重新塞回日志
     *
     * @param dataList
     */
    public void resetAllLog(List<LogDataBO> dataList) {
        logMachine.resetAllElement(dataList.stream().map(JSON::toJSONBytes).collect(Collectors.toList()));
    }

    /**
     * 确认落库
     *
     * @param serviceId
     */
    public void confirmLog(Integer index, String serviceId, List<Node> list) {
        if (!map.containsKey(index)) {
            List<LogConfirm> confirmList = new ArrayList<>();

            list.forEach(item -> {
                LogConfirm confirm = new LogConfirm();
                confirm.setServiceId(item.getServiceId());

                if (serviceId.equals(item.getServiceId())) {
                    confirm.setConfirmed(true);
                } else {
                    confirm.setConfirmed(false);
                }

                confirmList.add(confirm);
            });

            map.put(index, confirmList);
            return;
        }

        List<LogConfirm> confirmList = map.get(index);

        confirmList.stream().filter(item -> item.getServiceId().equals(serviceId)).findFirst().get().setConfirmed(true);
    }

    public long getLastIndex() {
        return logMachine.getLastIndex();
    }

    /**
     * 输出倒数几个日志
     *
     * @param count
     * @return
     */
    public List<LogDataBO> getLastLogByCount(Integer count) {
        List<LogDataBO> resultList = new ArrayList<>();

        long lastIndex = getLastIndex();

        while (count > 0 && count <= lastIndex) {
            resultList.add(JSON.parseObject(logMachine.getElement((int) lastIndex), LogDataBO.class));

            count--;
        }

        return resultList;
    }


    /**
     * 搂出所有日志
     *
     * @return
     */
    public List<LogDataBO> getAllLog() {
        List<LogDataBO> resultList = new ArrayList<>();

        long lastIndex = getLastIndex();

        int i = 1;

        while (i <= lastIndex) {
            resultList.add(JSON.parseObject(logMachine.getElement(i), LogDataBO.class));
            i++;
        }

        return resultList;
    }

    /**
     * 获取日志
     *
     * @param index
     * @return
     */
    public String getLog(Integer index) {
        return logMachine.getElement(index);
    }

    /**
     * 写入到日志
     */
    public void addLog(LogDataBO dataBO) {
        logMachine.addElement(JSON.toJSONString(dataBO).getBytes());
    }

    /**
     * 提交结果
     */
    @Data
    public static class LogConfirm {
        private String serviceId;

        private Boolean confirmed;
    }


}
