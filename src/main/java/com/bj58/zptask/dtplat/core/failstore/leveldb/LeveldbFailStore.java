package com.bj58.zptask.dtplat.core.failstore.leveldb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;

import com.bj58.zptask.dtplat.core.domain.KVPair;
import com.bj58.zptask.dtplat.core.failstore.AbstractFailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreException;
import com.bj58.zptask.dtplat.util.FileUtils;
import com.bj58.zptask.dtplat.util.JSONUtils;

/**
 * Only a single process (possibly multi-threaded) can access a particular
 * database at a time Robert HG (254963746@qq.com) on 5/21/15.
 */
public class LeveldbFailStore extends AbstractFailStore {

    private DB db;

    private Options options;
    // 保证同时只有一个线程修改
    private ReentrantLock lock = new ReentrantLock();

    public static final String name = "leveldb";

    public LeveldbFailStore(String storePath, String identity) {
        this(new File(storePath.concat(name).concat("/").concat(identity)));
        getLock(dbPath.getPath());
    }

    public LeveldbFailStore(File dbPath) {
        super(dbPath);
    }

    @Override
    protected void init() {
        options = new Options();
        options.createIfMissing(true);
        options.cacheSize(100 * 1024 * 1024); // 100M
    }

    protected String getName() {
        return name;
    }

    @Override
    public void open() throws FailStoreException {
        try {
            lock.lock();
            JniDBFactory.factory.repair(dbPath, options);
            db = JniDBFactory.factory.open(dbPath, options);
        } catch (IOException e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void put(String key, Object value) throws FailStoreException {
        try {
            String valueString = JSONUtils.toJSONString(value);
            db.put(key.getBytes("UTF-8"), valueString.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void delete(String key) throws FailStoreException {
        try {
            if (key == null) {
                return;
            }
            db.delete(key.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void delete(List<String> keys) throws FailStoreException {
        if (keys == null || keys.size() == 0) {
            return;
        }
        WriteBatch batch = db.createWriteBatch();
        try {

            for (String key : keys) {
                batch.delete(key.getBytes("UTF-8"));
            }
            db.write(batch);
        } catch (UnsupportedEncodingException e) {
            throw new FailStoreException(e);
        } finally {
            try {
                batch.close();
            } catch (IOException e) {
                throw new FailStoreException(e);
            }
        }
    }

    @Override
    public <T> List<KVPair<String, T>> fetchTop(int size, Type type) throws FailStoreException {
        Snapshot snapshot = db.getSnapshot();
        DBIterator iterator = null;
        try {
            List<KVPair<String, T>> list = new ArrayList<KVPair<String, T>>(size);
            ReadOptions options = new ReadOptions();
            options.snapshot(snapshot);
            iterator = db.iterator(options);
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                Map.Entry<byte[], byte[]> entry = iterator.peekNext();
                String key = new String(entry.getKey(), "UTF-8");
                T value = JSONUtils.parse(new String(entry.getValue(), "UTF-8"), type);
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
                try {
                    iterator.close();
                } catch (IOException e) {
                    throw new FailStoreException(e);
                }
            }
            try {
                snapshot.close();
            } catch (IOException e) {
                throw new FailStoreException(e);
            }
        }
    }

    @Override
    public void close() throws FailStoreException {
        try {
            if (db != null) {
                db.close();
            }
        } catch (IOException e) {
            throw new FailStoreException(e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    public void destroy() throws FailStoreException {
        try {
            close();
            JniDBFactory.factory.destroy(dbPath, options);
        } catch (IOException e) {
            throw new FailStoreException(e);
        } finally {
            if (fileLock != null) {
                fileLock.release();
            }
            FileUtils.delete(dbPath);
        }
    }

}
