package com.bj58.zptask.dtplat.commons;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class InjectorHolder {

    public static Injector injector;

    public static void init() {
        injector = Guice.createInjector(new BindingModel());
    }

    public static <T> T getInstance(Class<T> type) {
        return injector.getInstance(type);
    }

}
