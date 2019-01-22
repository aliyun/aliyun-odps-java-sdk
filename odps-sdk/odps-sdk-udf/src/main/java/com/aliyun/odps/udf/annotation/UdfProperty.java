package com.aliyun.odps.udf.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface UdfProperty {
  boolean isDeterministic() default false;
}

