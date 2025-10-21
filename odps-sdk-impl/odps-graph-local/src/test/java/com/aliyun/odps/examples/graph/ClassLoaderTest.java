package com.aliyun.odps.examples.graph;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.utils.ReflectionUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
@Ignore
public class ClassLoaderTest {

    @Test
    public void testJava8() {
        URLClassLoader loader = (URLClassLoader) Thread.currentThread()
            .getContextClassLoader();
        ArrayList<URL> cp = new ArrayList<URL>(Arrays.asList(loader.getURLs()));
        System.out.println(cp.stream().map(URL::getPath).collect(Collectors.toList()));
    }


    @Test
    public void testJava21() throws Exception {
        List<URL> urls = ReflectionUtils.getLoadedJars();
        System.out.println(urls.stream().map(URL::getPath).collect(Collectors.toList()));
    }
}
