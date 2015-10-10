package com.bj58.zptask.dtplat.annotation;

import java.lang.annotation.*;

/**
 * 表示字段不允许为空
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE })
public @interface NotNull {
}
