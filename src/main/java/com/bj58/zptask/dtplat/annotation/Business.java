package com.bj58.zptask.dtplat.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 业务接口注解
 * 怎样做到 只加一个注解 或是 只实现一个接口  就可以自动运行到此方法
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月19日 下午2:44:06
 * @see 
 * @since
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Business {

}
