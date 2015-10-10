package com.bj58.zptask.dtplat.tasktracker.business;

/**
 * 业务执行完后的返回结果
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月19日 上午11:29:54
 * @see 
 * @since
 */
public class BusinessResult {

    private int code;
    private String msg;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BusinessResult [code=" + code + ", msg=" + msg + "]";
    }

}
