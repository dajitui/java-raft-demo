package timer;

/**
 * 定时器触发
 */
public interface TimerHandler {

    /**
     * 超时时间
     *
     * @return
     */
    int getTimeout();

    /**
     * 触发定时器逻辑
     */
    void trigger();

}
