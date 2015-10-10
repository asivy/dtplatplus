package com.bj58.zptask.dtplat.test.proxy;

import java.security.Key;
import java.util.HashMap;
import java.util.Map;

import net.sf.cglib.core.KeyFactory;
import net.sf.cglib.core.Signature;
import net.sf.cglib.proxy.InterfaceMaker;
import net.sf.cglib.proxy.Mixin;
import net.sf.cglib.util.StringSwitcher;

import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

public class TestKeyFactory {

    @Test
    public void testKeyFactory() {
        try {
            SampleKeyFactory keyFactory = (SampleKeyFactory) KeyFactory.create(Key.class);
            Object key = keyFactory.newInstance("foo", 42);
            Map<Object, String> map = new HashMap<Object, String>();
            map.put(key, "Hello cglib!");
            Assert.assertEquals("Hello cglib!", map.get(keyFactory.newInstance("foo", 42)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMixin() {
        try {
            Mixin mixin = Mixin.create(new Class[] { Interface1.class, Interface2.class, MixinInterface.class }, new Object[] { new Class1(), new Class2() });
            MixinInterface mixinDelegate = (MixinInterface) mixin;
            Assert.assertEquals("first", mixinDelegate.first());
            Assert.assertEquals("second", mixinDelegate.second());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStringSwitcher() {
        try {
            String[] strings = new String[] { "one", "two" };
            int[] values = new int[] { 10, 20 };
            StringSwitcher stringSwitcher = StringSwitcher.create(strings, values, true);
            Assert.assertEquals(10, stringSwitcher.intValue("one"));
            Assert.assertEquals(20, stringSwitcher.intValue("two"));
            Assert.assertEquals(-1, stringSwitcher.intValue("three"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInterfaceMaker() {
        try {
            Signature signature = new Signature("foo", Type.DOUBLE_TYPE, new Type[] { Type.INT_TYPE });
            InterfaceMaker interfaceMaker = new InterfaceMaker();
            interfaceMaker.add(signature, new Type[0]);
            Class iface = interfaceMaker.create();
            Assert.assertEquals(1, iface.getMethods().length);
            Assert.assertEquals("foo", iface.getMethods()[0].getName());
            Assert.assertEquals(double.class, iface.getMethods()[0].getReturnType());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
