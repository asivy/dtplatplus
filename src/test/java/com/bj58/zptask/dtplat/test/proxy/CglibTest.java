package com.bj58.zptask.dtplat.test.proxy;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.CallbackHelper;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.FixedValue;
import net.sf.cglib.proxy.InvocationHandler;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import net.sf.cglib.proxy.NoOp;

import org.junit.Assert;
import org.junit.Test;

public class CglibTest {

    @Test
    public void fixedValue() {
        try {
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(SampleClass.class);
            enhancer.setCallback(new FixedValue() {
                @Override
                public Object loadObject() throws Exception {
                    return "Hello cglib!";
                }
            });
            SampleClass proxy = (SampleClass) enhancer.create();
            System.out.println(proxy.test(null));
            System.out.println(proxy.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void invocationHandler() {
        try {
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(SampleClass.class);
            enhancer.setCallback(new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    System.out.println(method.getDeclaringClass());
                    if (method.getDeclaringClass() != Object.class && method.getReturnType() == String.class) {
                        return "Hello cglib!";
                    } else {
                        throw new RuntimeException("Do not know what to do.");
                    }
                }
            });
            SampleClass proxy = (SampleClass) enhancer.create();
            Assert.assertEquals("Hello cglib!", proxy.test(null));
            Assert.assertNotEquals("Hello cglib!", proxy.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMethodInterceptor() {
        try {
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(SampleClass.class);
            enhancer.setCallback(new MethodInterceptor() {
                @Override
                public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                    if (method.getDeclaringClass() != Object.class && method.getReturnType() == String.class) {
                        return "Hello cglib!";
                    } else {
                        return proxy.invokeSuper(obj, args);
                    }
                }
            });
            SampleClass proxy = (SampleClass) enhancer.create();
            System.out.println(proxy.test(null));
            System.out.println(proxy.toString());
            System.out.println(proxy.hashCode());
            Assert.assertEquals("Hello cglib!", proxy.test(null));
            Assert.assertNotEquals("Hello cglib!", proxy.toString());
            proxy.hashCode(); // Does not throw an exception or result in an endless loop.
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCallbackFilter() {
        try {
            Enhancer enhancer = new Enhancer();
            CallbackHelper callbackHelper = new CallbackHelper(SampleClass.class, new Class[0]) {
                @Override
                protected Object getCallback(Method method) {
                    if (method.getDeclaringClass() != Object.class && method.getReturnType() == String.class) {
                        return new FixedValue() {
                            @Override
                            public Object loadObject() throws Exception {
                                return "Hello cglib!";
                            }
                        };
                    } else {
                        return NoOp.INSTANCE; // A singleton provided by NoOp.
                    }
                }
            };
            enhancer.setSuperclass(SampleClass.class);
            enhancer.setCallbackFilter(callbackHelper);
            enhancer.setCallbacks(callbackHelper.getCallbacks());
            SampleClass proxy = (SampleClass) enhancer.create();
            Assert.assertEquals("Hello cglib!", proxy.test(null));
            Assert.assertNotEquals("Hello cglib!", proxy.toString());
            proxy.hashCode(); // Does not throw an exception or result in an endless loop.
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
