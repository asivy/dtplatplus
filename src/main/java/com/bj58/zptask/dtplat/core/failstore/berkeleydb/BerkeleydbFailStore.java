package com.bj58.zptask.dtplat.core.failstore.berkeleydb;

import java.io.File;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.bj58.zptask.dtplat.core.domain.KVPair;
import com.bj58.zptask.dtplat.core.failstore.AbstractFailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreException;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.FileUtils;
import com.bj58.zptask.dtplat.util.JSONUtils;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Robert HG (254963746@qq.com) on 5/26/15.
 */
public class BerkeleydbFailStore extends AbstractFailStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(BerkeleydbFailStore.class);
    private Environment environment;
    private Database db;
    private EnvironmentConfig envConfig;
    private ReentrantLock lock = new ReentrantLock();
    private DatabaseConfig dbConfig;

    public BerkeleydbFailStore(File dbPath) {
        super(dbPath);
    }

    public static final String name = "berkeleydb";

    @Override
    protected void init() {
        try {
            envConfig = new EnvironmentConfig();
            // 如果不存在则创建一个
            envConfig.setAllowCreate(true);
            // 以只读方式打开，默认为false
            envConfig.setReadOnly(false);
            // 事务支持,如果为true，则表示当前环境支持事务处理，默认为false，不支持事务处理
            envConfig.setTransactional(true);
            // Configures the durability associated with transactions.
            envConfig.setDurability(Durability.COMMIT_SYNC);

            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(false);
            dbConfig.setTransactional(true);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    public BerkeleydbFailStore(String storePath, String identity) {
        this(new File(storePath.concat(name).concat("/").concat(identity)));
        getLock(dbPath.getPath());
    }

    @Override
    public void open() throws FailStoreException {
        try {
            lock.lock();
            environment = new Environment(dbPath, envConfig);
            db = environment.openDatabase(null, "lts", dbConfig);
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void put(String key, Object value) throws FailStoreException {
        try {
            String valueString = JSONUtils.toJSONString(value);
            OperationStatus status = db.put(null, new DatabaseEntry(key.getBytes("UTF-8")), new DatabaseEntry(valueString.getBytes("UTF-8")));
        } catch (Exception e) {
            throw new FailStoreException(e);
        }
    }

    @Override
    public void delete(String key) throws FailStoreException {
        try {
            DatabaseEntry delKey = new DatabaseEntry();
            delKey.setData(key.getBytes("UTF-8"));
            OperationStatus status = db.delete(null, delKey);
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
        Cursor cursor = null;
        try {
            List<KVPair<String, T>> list = new ArrayList<KVPair<String, T>>();

            cursor = db.openCursor(null, CursorConfig.DEFAULT);
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundValue = new DatabaseEntry();
            while (cursor.getNext(foundKey, foundValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                String key = new String(foundKey.getData(), "UTF-8");
                String valueString = new String(foundValue.getData(), "UTF-8");

                T value = JSONUtils.parse(valueString, type);
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
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    LOGGER.warn("close cursor failed! ", e);
                }
            }
        }
    }

    @Override
    public void close() throws FailStoreException {
        try {
            if (db != null) {
                db.close();
            }
            if (environment != null && environment.isValid()) {
                environment.cleanLog();
                environment.close();
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
            if (environment != null) {
                environment.removeDatabase(null, db.getDatabaseName());
                environment.close();
            }
        } catch (Exception e) {
            throw new FailStoreException(e);
        } finally {
            if (fileLock != null) {
                fileLock.release();
            }
            FileUtils.delete(dbPath);
        }
    }

    @Override
    protected String getName() {
        return name;
    }
}
