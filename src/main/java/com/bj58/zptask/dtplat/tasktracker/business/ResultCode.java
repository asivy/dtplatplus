package com.bj58.zptask.dtplat.tasktracker.business;

public enum ResultCode {

    SUCCESS(0), //
    EXCEPTION(1), //
    FAIL(2);

    private int value;

    private ResultCode(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
