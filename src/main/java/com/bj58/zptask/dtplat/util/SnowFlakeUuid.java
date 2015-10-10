package com.bj58.zptask.dtplat.util;

import java.util.Random;

import org.apache.log4j.Logger;

/**
 * 在分布式环境下快速生成 唯一、递增、长度固定  最大19位ID
 * twitter-snowflake 算法：毫秒级时间41位+机器ID10位+毫秒内序列12位
 * 在一毫秒内可以达以1000的并发
 * 如果要达到强单调递增  可以走架构部的IDC服务
 * 
 * @see http://www.dengchuanhua.com/132.html
 * @author Ivy
 * @version 1.0 
 * @date  2014年7月14日 下午4:21:04
 * @see 
 * @since
 */
public class SnowFlakeUuid {

    protected static final Logger logger = Logger.getLogger(SnowFlakeUuid.class);

    private long workerId;
    private long datacenterId;
    private long sequence = 0L;

    private long twepoch = 1405406935291L;

    private long workerIdBits = 5L;
    private long datacenterIdBits = 5L;
    private long maxWorkerId = -1L ^ (-1L << workerIdBits);
    private long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
    private long sequenceBits = 12L;

    private long workerIdShift = sequenceBits;
    private long datacenterIdShift = sequenceBits + workerIdBits;
    private long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    private long sequenceMask = -1L ^ (-1L << sequenceBits);

    private long lastTimestamp = -1L;

    public static SnowFlakeUuid getInstance() {
        return SnowFlakeHolder.instance;
    }

    private static class SnowFlakeHolder {
        private static final SnowFlakeUuid instance = new SnowFlakeUuid();
    }

    private SnowFlakeUuid() {
        Random r = new Random();

        workerId = Math.abs(r.nextLong()) % 8;
        datacenterId = workerId;
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        logger.info(String.format("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId));
    }

    public synchronized long nextId() {
        long timestamp = timeGen();

        //同一台机器 ，时间不能往前更改
        if (timestamp < lastTimestamp) {
            logger.error(String.format("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp));
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        //同一毫秒内的sequence变化
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - twepoch) << timestampLeftShift) | (datacenterId << datacenterIdShift) | (workerId << workerIdShift) | sequence;
        //        return (datacenterId << datacenterIdShift) | (workerId << workerIdShift) | sequence;
    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    //    public static synchronized void init(long workerId, long datacenterId) {
    //        if (instance == null) {
    //            instance = new SnowFlakeUuid(workerId, datacenterId);
    //        }
    //    }

    //    public SnowFlakeUuid(long workerId, long datacenterId) {
    //        if (workerId > maxWorkerId || workerId < 0) {
    //            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
    //        }
    //        if (datacenterId > maxDatacenterId || datacenterId < 0) {
    //            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
    //        }
    //        this.workerId = workerId;
    //        this.datacenterId = datacenterId;
    //        logger.info(String.format("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId));
    //    }

}
