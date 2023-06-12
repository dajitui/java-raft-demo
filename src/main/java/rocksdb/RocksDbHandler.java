package rocksdb;

import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.SneakyThrows;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
public class RocksDbHandler {

    private String path = "D:\\baibu\\raft-demo\\src\\main\\resources\\stateMachine";
    private String nodePath;

    //状态机
    private static RocksDB rocksDB;

    private Long lastIndex;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public RocksDbHandler(String nodePath) {
        this.nodePath = nodePath;
        init();
    }

    public Long getLastIndex() {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        long result = this.lastIndex;
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
//        RocksDB.destroyDB(path + "/" + nodePath, options);

        rocksDB = RocksDB.open(options, path + "\\" + nodePath);
        this.lastIndex = 0L;
    }

    @SneakyThrows
    public void resetAllElement(List<byte[]> dataList) {
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        dataList.forEach(item -> {
            try {
                rocksDB.put(String.valueOf(this.lastIndex).getBytes(), item);
                this.lastIndex = this.lastIndex + 1;
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        });
        writeLock.unlock();
    }

    @SneakyThrows
    public String getElement(Integer index) {
        return new String(Optional.ofNullable(rocksDB.get(String.valueOf(index).getBytes())).orElse(StrUtil.EMPTY.getBytes()));
    }

    @SneakyThrows
    public void addElement(byte[] data) {
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        this.lastIndex = this.lastIndex + 1;
        rocksDB.put(String.valueOf(this.lastIndex).getBytes(), data);
        writeLock.unlock();
    }

}
