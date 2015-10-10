package com.bj58.zptask.dtplat.test.java;

import org.junit.Test;

public class StringTest {

    @Test
    public void split() {
        String s = "||";
        System.out.println(s);
        String[] ss = s.split("//|");
        for (String str : ss) {
            System.out.println(str);
        }
    }
}
