package com.aliyun.odps.utils;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class NameSpaceSchemaUtilsTest extends TestCase {

    @Test
    public void testGetFullName() {
        Assert.assertEquals("`a`.`b`.`c`", NameSpaceSchemaUtils.getFullName("a", "b", "c"));
        Assert.assertEquals("`a`.`c`", NameSpaceSchemaUtils.getFullName("a", null, "c"));
        Assert.assertEquals("`a`.`c`", NameSpaceSchemaUtils.getFullName("a", " ", "c"));
    }

    @Test
    public void testSetSchemaFlagInHints() {
        Assert.assertEquals(null, NameSpaceSchemaUtils.setSchemaFlagInHints(null, null));
        Assert.assertEquals(2, NameSpaceSchemaUtils.setSchemaFlagInHints(null, "name").size());

        HashMap<String, String> hints = new HashMap<>();
        hints.put("a", "b");
        Assert.assertEquals(1, NameSpaceSchemaUtils.setSchemaFlagInHints(hints, null).size());
        Assert.assertEquals(3, NameSpaceSchemaUtils.setSchemaFlagInHints(hints, "name").size());
    }

    @Test
    public void testInitParamsWithSchema() {
        Assert.assertTrue(NameSpaceSchemaUtils.initParamsWithSchema(null).isEmpty());
        Assert.assertTrue(NameSpaceSchemaUtils.initParamsWithSchema(" ").isEmpty());
        Assert.assertFalse(NameSpaceSchemaUtils.initParamsWithSchema("schemaName").isEmpty());
    }

    @Test
    public void testIsSchemaEnabled() {
        Assert.assertFalse(NameSpaceSchemaUtils.isSchemaEnabled(null));
        Assert.assertFalse(NameSpaceSchemaUtils.isSchemaEnabled(""));
        Assert.assertFalse(NameSpaceSchemaUtils.isSchemaEnabled("  "));
        Assert.assertTrue(NameSpaceSchemaUtils.isSchemaEnabled("name"));
    }
}