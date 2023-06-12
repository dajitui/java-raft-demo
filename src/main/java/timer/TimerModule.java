package timer;

import lombok.SneakyThrows;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 定时器构造器
 */
public class TimerModule {

    //定时器线程池
    private ScheduledThreadPoolExecutor executor;

    //定时器处理器
    private TimerHandler handler;

    //执行中的任务
    private ScheduledFuture<?> future;

    public TimerHandler getHandler() {
        return handler;
    }

    /**
     * 初始化内容
     */
    public TimerModule build(TimerHandler handler) {
        TimerModule timerModule = new TimerModule();

        int cpuThread = Runtime.getRuntime().availableProcessors();
        timerModule.executor = new ScheduledThreadPoolExecutor(Math.min(3 * cpuThread, 20));

        timerModule.handler = handler;

        return timerModule;
    }

    public void resetHandler(boolean cancel) {
        if(cancel){
            future.cancel(true);
        }
        //重新提交
        future = executor.schedule(() -> handler.trigger(), handler.getTimeout(), TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    public void addTimeoutTaskRunNow() {
        if (executor == null || handler == null) {
            throw new Exception("定时器没有初始化");
        }

        executor.schedule(() -> handler.trigger(), 0, TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    public void addTimeoutTask() {
        if (executor == null || handler == null) {
            throw new Exception("定时器没有初始化");
        }

        future = executor.schedule(() -> handler.trigger(), handler.getTimeout(), TimeUnit.MILLISECONDS);
    }

}
