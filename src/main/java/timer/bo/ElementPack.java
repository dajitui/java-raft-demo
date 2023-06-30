package timer.bo;

import log.LogDataBO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElementPack {

    private boolean forward;

    //谁丢过来的心跳
    private String serviceId;

    //当前任期
    private int term;

    //日志最后index
    private long lastLogIndex;

    //日志最后index
    private long lastCommitIndex;

    //插入数据
    private String data;

    //用于数据对齐
    private List<LogDataBO> leaderDataList;

    //响应结果
    private ElementPackResponse response;

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @Data
    public static class ElementPackResponse {

        //应用结果
        private Boolean result;

        //插入数据
        private List<LogDataBO> dataList;

    }

}
