package fsm;

import com.alibaba.fastjson.JSON;
import log.LogDataBO;
import rocksdb.RocksDbHandler;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 状态机
 */
public class StateMachine extends RocksDbHandler {

    public StateMachine(String serviceId) {
        super("StateMachine" + serviceId);
    }

    /**
     * 应用某个东西到状态机
     */
    public void apply(String objectStr) {
        addElement(objectStr.getBytes());
    }

    public void resetAllLog(List<LogDataBO> dataList) {
        resetAllElement(dataList.stream().map(JSON::toJSONBytes).collect(Collectors.toList()));
    }
}
