package com.bj58.zptask.dtplat.rpc;

import com.bj58.zptask.dtplat.exception.RemotingCommandFieldCheckException;

/**
 * RemotingCommand中自定义字段反射对象的公共接口
 */
public interface CommandBody {

    public void checkFields() throws RemotingCommandFieldCheckException;
}
