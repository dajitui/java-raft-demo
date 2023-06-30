package rocksdb;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.SneakyThrows;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class RocksDbHandler {

    private String path = "D:\\baibu\\raft-demo\\src\\main\\resources\\stateMachine";
    private String nodePath;

    private ReentrantReadWriteLock.WriteLock writeLock;

    private ReentrantReadWriteLock.ReadLock readLock;

    //状态机
    private static RocksDB rocksDB;

    private volatile AtomicInteger lastIndex;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public RocksDbHandler(String nodePath) {
        this.nodePath = nodePath;
        this.writeLock = this.lock.writeLock();
        this.readLock = this.lock.readLock();
        init();
    }

    public Long getLastIndex() {
        readLock.lock();
        long result = this.lastIndex.get();
        readLock.unlock();
        return result;
    }

    public static RocksDB getRocksDB() {
        return rocksDB;
    }

    @SneakyThrows
    private void init() {
        RocksDB.loadLibrary();

        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB.destroyDB(path + "\\" + nodePath, options);
        FileUtil.clean(path + "\\" + nodePath);

        rocksDB = RocksDB.open(options, path + "\\" + nodePath);
        this.lastIndex = new AtomicInteger(0);
    }

    @SneakyThrows
    public void resetAllElement(List<byte[]> dataList) {
        dataList.forEach(this::addElement);
    }

    @SneakyThrows
    public String getElement(Integer index) {
        return new String(Optional.ofNullable(rocksDB.get(String.valueOf(index).getBytes())).orElse(StrUtil.EMPTY.getBytes()));
    }

    @SneakyThrows
    public void addElement(byte[] data) {
        writeLock.lock();
        this.lastIndex.addAndGet(1);
        rocksDB.put(String.valueOf(this.lastIndex).getBytes(), data);
        writeLock.unlock();
    }

}
