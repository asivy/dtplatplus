package com.bj58.zptask.dtplat.rpc.netty5;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * netty 服务交互的实体类
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月7日 下午4:39:10
 * @see 
 * @since
 */
public final class NettyMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private static AtomicLong RequestId = new AtomicLong(0);

    private int type;// 消息类型 对应messagetype中的枚举类型
    private int flag = 0;
    private long jobId;
    //以下两个用哪个做序列标识都成
    private Long timestamp = System.currentTimeMillis();
    private long opaque = RequestId.getAndIncrement();

    private String nodeGroup;
    private String nodeType;
    private String identity;//这个是节点组的唯一标识符 可能是task也可能是job

    private String jsonobj;

    private int version = 0;

    public String getUuid() {
        return String.format("%s-%d", getIdentity(), getOpaque());
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getOpaque() {
        return opaque;
    }

    public void setOpaque(long opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getNodeGroup() {
        return nodeGroup;
    }

    public void setNodeGroup(String nodeGroup) {
        this.nodeGroup = nodeGroup;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getJsonobj() {
        return jsonobj;
    }

    public void setJsonobj(String jsonobj) {
        this.jsonobj = jsonobj;
    }

}
