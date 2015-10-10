package com.bj58.zptask.dtplat.test.event;

import java.util.ArrayList;
import java.util.List;

import com.google.common.eventbus.EventBus;

public class EventMain {

    public static void main(String[] args) {
        EventBus eventBus = new EventBus();
        eventBus.register(new MessageScreen());
        //        eventBus.post("Hello Screen");
        List<String> list = new ArrayList<String>();
        list.add("hello");
        list.add("world");
        eventBus.post(list);

    }
}
