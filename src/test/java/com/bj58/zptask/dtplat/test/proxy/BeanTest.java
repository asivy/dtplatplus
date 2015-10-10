package com.bj58.zptask.dtplat.test.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;
import net.sf.cglib.beans.BulkBean;
import net.sf.cglib.beans.ImmutableBean;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import net.sf.cglib.reflect.MethodDelegate;
import net.sf.cglib.util.ParallelSorter;

import org.junit.Assert;
import org.junit.Test;

public class BeanTest {

    @Test(expected = IllegalStateException.class)
    public void testImmutableBean() {
        SampleBean bean = new SampleBean();
        bean.setValue("Hello world!");
        SampleBean immutableBean = (SampleBean) ImmutableBean.create(bean);
        Assert.assertEquals("Hello world!", immutableBean.getValue());
        bean.setValue("Hello world, again!");
        Assert.assertEquals("Hello world, again!", immutableBean.getValue());
        immutableBean.setValue("Hello cglib!"); // Causes exception.
    }

    @Test
    public void testBeanGenerator() {
        try {
            BeanGenerator beanGenerator = new BeanGenerator();
            beanGenerator.addProperty("value", String.class);
            Object myBean = beanGenerator.create();

            Method setter = myBean.getClass().getMethod("setValue", String.class);
            setter.invoke(myBean, "Hello cglib!");
            Method getter = myBean.getClass().getMethod("getValue");
            Assert.assertEquals("Hello cglib!", getter.invoke(myBean));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBeanCopier() {
        try {
            BeanCopier copier = BeanCopier.create(SampleBean.class, OtherSampleBean.class, false);
            SampleBean bean = new SampleBean();
            bean.setValue("Hello cglib!");
            OtherSampleBean otherBean = new OtherSampleBean();
            copier.copy(bean, otherBean, null);
            System.out.println(otherBean.getValue());
            Assert.assertEquals("Hello cglib!", otherBean.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBulkBean() {
        try {
            BulkBean bulkBean = BulkBean.create(SampleBean.class, new String[] { "getValue" }, new String[] { "setValue" }, new Class[] { String.class });
            SampleBean bean = new SampleBean();
            bean.setValue("Hello world!");
            Assert.assertEquals(1, bulkBean.getPropertyValues(bean).length);
            Assert.assertEquals("Hello world!", bulkBean.getPropertyValues(bean)[0]);
            bulkBean.setPropertyValues(bean, new Object[] { "Hello cglib!" });
            Assert.assertEquals("Hello cglib!", bean.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBeanMapper() {
        try {
            SampleBean bean = new SampleBean();
            BeanMap map = BeanMap.create(bean);
            bean.setValue("Hello cglib!");
            Assert.assertEquals("Hello cglib", map.get("value"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMethodDelegate() {
        try {
            SampleBean bean = new SampleBean();
            bean.setValue("Hello cglib!");
            BeanDelegate delegate = (BeanDelegate) MethodDelegate.create(bean, "getValue", BeanDelegate.class);
            Assert.assertEquals("Hello world!", delegate.getValueFromDelegate());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testParallelSorter() {
        try {
            Integer[][] value = { { 4, 3, 9, 0 }, { 2, 1, 6, 0 } };
            ParallelSorter.create(value).mergeSort(0);
            for (Integer[] row : value) {
                int former = -1;
                for (int val : row) {
                    Assert.assertTrue(former < val);
                    former = val;
                    System.out.println(former);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFastClass() {
        try {
            FastClass fastClass = FastClass.create(SampleBean.class);
            FastMethod fastMethod = fastClass.getMethod(SampleBean.class.getMethod("getValue"));
            SampleBean myBean = new SampleBean();
            myBean.setValue("Hello cglib!");
            Assert.assertEquals("Hello cglib!", fastMethod.invoke(myBean, new Object[0]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
