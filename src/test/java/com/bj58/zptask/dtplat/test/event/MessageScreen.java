package com.bj58.zptask.dtplat.test.event;

import java.util.List;

import com.google.common.eventbus.Subscribe;

public class MessageScreen {
    @Subscribe
    public void printMessage(String message) {
        System.out.println(message);
    }

    @Subscribe
    public void printAll(List<String> list) {
        for (String str : list) {
            System.out.println(str);
        }
    }
}
