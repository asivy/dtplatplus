package com.bj58.zptask.dtplat.core.protocol.command;

import com.bj58.zptask.dtplat.core.domain.BizLog;

import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 3/27/15.
 */
public class BizLogSendRequest extends AbstractCommandBody {

    private List<BizLog> bizLogs;

    public List<BizLog> getBizLogs() {
        return bizLogs;
    }

    public void setBizLogs(List<BizLog> bizLogs) {
        this.bizLogs = bizLogs;
    }
}
