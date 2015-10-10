package com.bj58.zptask.dtplat.core.failstore.rocksdb;

import java.io.File;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Filter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import com.bj58.zptask.dtplat.core.domain.KVPair;
import com.bj58.zptask.dtplat.core.failstore.AbstractFailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreException;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.FileUtils;
import com.bj58.zptask.dtplat.util.JSONUtils;

/**
 * Robert HG (254963746@qq.com) on 5/27/15.
 */
public class RocksdbFailStore extends AbstractFailStore {

    private RocksDB db = null;
    private Options options;
    private ReentrantLock lock = new ReentrantLock();

    public RocksdbFailStore(File dbPath) {
        super(dbPath);
    }

    public static final String name = "rocksdb";

    @Override
    protected void init() {
        options = new Options();
        options.setCreateIfMissing(true).setWriteBufferSize(8 * SizeUnit.KB).setMaxWriteBufferNumber(3).setMaxBackgroundCompactions(10).setCompressionType(CompressionType.SNAPPY_COMPRESSION).setCompactionStyle(CompactionStyle.UNIVERSAL);

        Filter bloomFilter = new BloomFilter(10);
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(64 * SizeUnit.KB).setFilter(bloomFilter).setCacheNumShardBits(6).setBlockSizeDeviation(5).setBlockRestartInterval(10).setCacheIndexAndFilterBlocks(true).setHashIndexAllowCollision(false).setBlockCacheCompressedSize(64 * SizeUnit.KB).setBlockCacheCompressedNumShardBits(10);

        options.setTableFormatConfig(tableConfig);
    }

    public RocksdbFailStore(String storePath, String identity) {
        this(new File(storePath.concat(name).concat("/").concat(identity)));
        getLock(dbPath.getPath());
    }

    @Override
    protected String getName() {
        return name;
    }

    @Override
    public void open() throws FailStoreException {
        try {
            lock.lock();
            db = RocksDB.open(options, dbPath.getPath());
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void put(String key, Object value) throws FailStoreException {
        String valueString = JSONUtils.toJSONString(value);
        WriteOptions writeOpts = new WriteOptions();
        try {
            writeOpts.setSync(true);
            writeOpts.setDisableWAL(true);
            db.put(writeOpts, key.getBytes("UTF-8"), valueString.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new FailStoreException(e);
        } finally {
            writeOpts.dispose();
        }
    }

    @Override
    public void delete(String key) throws FailStoreException {
        try {
            db.remove(key.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void delete(List<String> keys) throws FailStoreException {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        for (String key : keys) {
            delete(key);
        }
    }

    @Override
    public <T> List<KVPair<String, T>> fetchTop(int size, Type type) throws FailStoreException {
        RocksIterator iterator = null;
        try {
            List<KVPair<String, T>> list = new ArrayList<KVPair<String, T>>(size);
            iterator = db.newIterator();
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                iterator.status();
                String key = new String(iterator.key(), "UTF-8");
                T value = JSONUtils.parse(new String(iterator.value(), "UTF-8"), type);
                KVPair<String, T> pair = new KVPair<String, T>(key, value);
                list.add(pair);
                if (list.size() >= size) {
                    break;
                }
            }
            return list;
        } catch (Exception e) {
            throw new FailStoreException(e);
        } finally {
            if (iterator != null) {
                iterator.dispose();
            }
        }
    }

    @Override
    public void close() throws FailStoreException {
        try {
            if (db != null) {
                db.close();
            }
        } catch (Exception e) {
            throw new FailStoreException(e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public void destroy() throws FailStoreException {
        try {
            db.close();
            options.dispose();
        } catch (Exception e) {
            throw new FailStoreException(e);
        } finally {
            if (fileLock != null) {
                fileLock.release();
            }
            FileUtils.delete(dbPath);
        }
    }
}
