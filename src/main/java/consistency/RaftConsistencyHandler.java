package consistency;

import cn.hutool.core.collection.CollectionUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一致性处理器
 */
public class RaftConsistencyHandler {

    //投票的结果，key 任期，value是结果
    private Map<Integer, List<NodeVoteResult>> voteResultMap = new HashMap<>();

    //投给谁
    public void voteToTho(String serviceId, Integer term) {
        NodeVoteResult result = NodeVoteResult.builder().voteResult(true).serviceId(serviceId).build();

        if (!voteResultMap.containsKey(term)) {
            List<NodeVoteResult> list = new ArrayList<>();
            list.add(result);

            voteResultMap.put(term, list);
            return;
        }

        voteResultMap.get(term).add(result);
    }


    /**
     * 最终授权为leader判断
     *
     * @return
     */
    public boolean isGrantedToLeader(Integer term, Integer totalCount) {
        List<NodeVoteResult> resultList = voteResultMap.get(term);

        if (CollectionUtil.isEmpty(resultList)) {
            return false;
        }

        long grantCount = resultList.stream().filter(NodeVoteResult::getVoteResult).count();

        return totalCount / 2 + 1 < grantCount;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class NodeVoteResult {
        //唯一标识
        private String serviceId;

        //投票结果
        private Boolean voteResult;
    }

}
