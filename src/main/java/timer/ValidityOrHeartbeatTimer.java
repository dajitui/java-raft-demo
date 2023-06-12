package timer;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 有效期定时器，如果过期需要进行vote
 * 心跳定时器，如果是leader的话定期给foller心跳
 */
public class ValidityOrHeartbeatTimer implements TimerHandler {

    //超时时间
    private int timeout;

    public ValidityOrHeartbeatTimer(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public int getTimeout() {
        return timeout + ThreadLocalRandom.current().nextInt(1000, 3000);
    }

    @Override
    public void trigger() {

    }

}
