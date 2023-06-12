package timer.bo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 投票包
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VotePack {

    //谁丢过来的心跳
    private String serviceId;

    //当前任期
    private int term;

    //日志最后index
    private long lastLogIndex;

    //回复
    private VotePackResponse response;

    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    @Data
    public static class VotePackResponse {

        //投票结果
        private boolean voteResult;

    }


}
