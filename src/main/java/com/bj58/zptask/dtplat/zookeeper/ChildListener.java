package com.bj58.zptask.dtplat.zookeeper;

import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 7/8/14.
 */
public interface ChildListener {

    void childChanged(String path, List<String> children);

}