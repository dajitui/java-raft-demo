package log;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 日志数据内容
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LogDataBO {

    //任期
    private Long term;

    //数据
    private String data;

}
