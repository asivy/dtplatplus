package com.bj58.zptask.dtplat.exception;

/**
 * @author Robert HG (254963746@qq.com) on 5/20/15.
 */
public class JobQueueException extends RuntimeException {


    private static final long serialVersionUID = -4512927085082188177L;

    public JobQueueException(String message) {
        super(message);
    }

    public JobQueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobQueueException(Throwable cause) {
        super(cause);
    }
}
