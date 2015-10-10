package com.bj58.zptask.dtplat.test.guava;

import org.junit.Test;

import com.google.common.base.Preconditions;

public class PreconditionTest {

    @Test
    public void checknotnull() {
        try {
            Preconditions.checkNotNull(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
