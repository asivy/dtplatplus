package com.bj58.zptask.dtplat.test.classs;

import java.util.Set;

import org.junit.Test;

import com.bj58.zptask.dtplat.annotation.Business;
import com.bj58.zptask.dtplat.util.PackageScan;

public class ScanTest {

    @Test
    public void scanAll() {
        Set<Class<?>> all = PackageScan.scanClasses("com.bj58.zptask");
        for (Class<?> cls : all) {
            if (cls.isAnnotationPresent(Business.class)) {
                String name = cls.getName();
                System.out.println(name.substring(name.lastIndexOf(".") + 1, name.length()) + "---" + name);
            }
        }
    }
}
