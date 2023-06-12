package timer.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 心跳包
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HeartBeatPack {

    //谁丢过来的心跳
    private String serviceId;

    //当前任期
    private int term;

    //日志最后index
    private long lastLogIndex;

    //日志最后index
    private long lastCommitIndex;

}
