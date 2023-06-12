package fsm;

import com.alibaba.fastjson.JSON;
import log.LogDataBO;
import rocksdb.RocksDbHandler;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 状态机
 */
public class StateMachine {

    //状态机实际处理器
    private static RocksDbHandler fsm;

    public StateMachine(String serviceId) {
        //初始化状态机 rocksdb
        fsm = new RocksDbHandler("StateMachine"+serviceId);
    }

    public RocksDbHandler getFsm() {
        return fsm;
    }

    /**
     * 应用某个东西到状态机
     */
    public void apply(String objectStr) {
        fsm.addElement(objectStr.getBytes());
    }

    public void resetAllLog(List<LogDataBO> dataList) {
        fsm.resetAllElement(dataList.stream().map(JSON::toJSONBytes).collect(Collectors.toList()));
    }
}
