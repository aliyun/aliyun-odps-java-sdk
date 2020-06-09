package com.aliyun.odps.tunnel;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.util.Utils;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.math.RandomUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamUploadTest {
    @Test
    public void UploadTable() throws OdpsException, IOException {
        final String projectName = Utils.getProjectName();
        final String tableName = Utils.getRandomTableName();
        Utils.dropTableIfExists(tableName);
        Utils.exeSql("create table " + tableName + " (col1 string);");
        TableTunnel tunnel = Utils.getTunnelInstance();
        Record record;
        TableTunnel.StreamUploadSession session = tunnel.createStreamUploadSession(projectName, tableName);
        record = session.newRecord();
        record.setString("col1", "value");
        TableTunnel.StreamRecordPack pack = session.newRecordPack();
        int packNum = RandomUtils.nextInt(9) + 1;
        int recordsPerPack = RandomUtils.nextInt(1000);
        for (int i = 0; i < packNum; ++i) {
            for (int j = 0; j < recordsPerPack; ++j) {
                pack.append(record);
            }
            System.out.println(pack.flush());
        }

        TableTunnel.DownloadSession down = tunnel.createDownloadSession(projectName, tableName);
        System.out.println(down.getId());
        Assert.assertEquals(packNum * recordsPerPack, down.getRecordCount());
        RecordReader reader = down.openRecordReader(0, down.getRecordCount());
        record = reader.read();
        int count = 0;
        while (record != null) {
            Assert.assertEquals("value", record.getString("col1"));
            ++count;
            record = reader.read();
        }
        Assert.assertEquals(packNum * recordsPerPack, count);

        SQLTask task = new SQLTask();
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.jobconf.odps2", "true");
        task.setName("SQLTask");
        task.setQuery("select * from " + tableName + ";");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        Odps odps = OdpsTunnelTestUtils.newDefaultOdps();
        Instance instance = odps.instances().create(task);
        System.out.println(odps.logview().generateLogView(instance, 24));
        instance.waitForSuccess();

        List<Record> records = SQLTask.getResult(instance, "SQLTask");
        Assert.assertNotNull(records);
        Assert.assertEquals(packNum * recordsPerPack, records.size());
        for (Record r: records) {
            Assert.assertEquals("value", r.getString("col1"));
        }

        ResultSet resultSet = SQLTask.getResultSet(instance, "SQLTask");
        count = 0;
        while (resultSet.hasNext()) {
            Assert.assertEquals("value", resultSet.next().getString("col1"));
            count++;
        }
        Assert.assertEquals(packNum * recordsPerPack, count);

        task = new SQLTask();
        task.setName("SQLTask");
        task.setQuery("select count(*) from " + tableName + ";");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        instance = odps.instances().create(task);
        System.out.println(odps.logview().generateLogView(instance, 24));
        instance.waitForSuccess();
        records = SQLTask.getResult(instance, "SQLTask");
        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(packNum * recordsPerPack, Integer.valueOf(records.get(0).getString(0)).intValue());
    }

    @Test
    public void UploadPartition() throws OdpsException, IOException {
        final String projectName = Utils.getProjectName();
        final String tableName = Utils.getRandomTableName();
        Utils.dropTableIfExists(tableName);
        Utils.exeSql("create table " + tableName + " (col1 string) partitioned by (pt string);");
        Utils.exeSql("alter table " + tableName + " add partition(pt='a');");
        System.out.println(tableName);
        TableTunnel tunnel = Utils.getTunnelInstance();
        Record record;
        TableTunnel.StreamUploadSession session = tunnel.createStreamUploadSession(projectName, tableName, new PartitionSpec("pt='a'"));
        record = session.newRecord();
        record.setString("col1", "value");
        int packNum = RandomUtils.nextInt(9) + 1;
        int recordsPerPack = RandomUtils.nextInt(1000);
        TableTunnel.StreamRecordPack pack = session.newRecordPack();
        for (int i = 0; i < packNum; ++i) {
            for (int j = 0; j < recordsPerPack; ++j) {
                pack.append(record);
            }
            System.out.println(pack.flush());
        }

        tunnel = Utils.getTunnelInstance();
        TableTunnel.DownloadSession down = tunnel.createDownloadSession(projectName, tableName, new PartitionSpec("pt=a"));
        System.out.println(down.getId());
        Assert.assertEquals(packNum * recordsPerPack, down.getRecordCount());
        RecordReader reader = down.openRecordReader(0, down.getRecordCount());
        record = reader.read();
        int count = 0;
        while (record != null) {
            Assert.assertEquals("value", record.getString("col1"));
            ++count;
            record = reader.read();
        }
        Assert.assertEquals(packNum * recordsPerPack, count);

        SQLTask task = new SQLTask();
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.jobconf.odps2", "true");
        task.setName("SQLTask");
        task.setQuery("select * from " + tableName + ";");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        Odps odps = OdpsTunnelTestUtils.newDefaultOdps();
        Instance instance = odps.instances().create(task);
        System.out.println(odps.logview().generateLogView(instance, 24));
        instance.waitForSuccess();

        List<Record> records = SQLTask.getResult(instance, "SQLTask");
        Assert.assertNotNull(records);
        Assert.assertEquals(packNum * recordsPerPack, records.size());
        for (Record r: records) {
            Assert.assertEquals("value", r.getString("col1"));
            Assert.assertEquals("a", r.getString("pt"));
        }

        ResultSet resultSet = SQLTask.getResultSet(instance, "SQLTask");
        count = 0;
        while (resultSet.hasNext()) {
            Record r = resultSet.next();
            Assert.assertEquals("value", r.getString("col1"));
            Assert.assertEquals("a", r.getString("pt"));
            count++;
        }
        Assert.assertEquals(packNum * recordsPerPack, count);

        task = new SQLTask();
        task.setName("SQLTask");
        task.setQuery("select count(*) from " + tableName + ";");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        instance = odps.instances().create(task);
        System.out.println(odps.logview().generateLogView(instance, 24));
        instance.waitForSuccess();
        records = SQLTask.getResult(instance, "SQLTask");
        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(packNum * recordsPerPack, Integer.valueOf(records.get(0).getString(0)).intValue());
        Utils.dropTableIfExists(tableName);
    }

    @Test
    public void ProjectNotExist() throws OdpsException, IOException {
        final String projectName = Utils.getRandomProjectName();
        final String tableName = Utils.getRandomTableName();
        System.out.println("Project: " + projectName);
        System.out.println("Table: " + tableName);
        TableTunnel tunnel = Utils.getTunnelInstance();
        try {
            tunnel.createStreamUploadSession(projectName, tableName, new PartitionSpec("pt='a'"));
            Assert.fail("A Tunnel Exception should be thrown");
        } catch (TunnelException e) {
            Assert.assertThat(e.getMessage(), CoreMatchers.containsString("NoSuchProject"));
        }
    }

    @Test
    public void TableNotExist() throws OdpsException, IOException {
        final String projectName = Utils.getProjectName();
        final String tableName = Utils.getRandomTableName();
        System.out.println(tableName);
        Utils.dropTableIfExists(tableName);
        TableTunnel tunnel = Utils.getTunnelInstance();
        try {
            tunnel.createStreamUploadSession(projectName, tableName, new PartitionSpec("pt='a'"));
            Assert.fail("A Tunnel Exception should be thrown");
        } catch (TunnelException e) {
            Assert.assertThat(e.getMessage(), CoreMatchers.containsString("NoSuchTable"));
        }
    }

    @Test
    public void PartitionNotExist() throws OdpsException, IOException {
        final String projectName = Utils.getProjectName();
        final String tableName = Utils.getRandomTableName();
        Utils.dropTableIfExists(tableName);
        Utils.exeSql("create table " + tableName + " (col1 string) partitioned by (pt string);");
        System.out.println(tableName);
        TableTunnel tunnel = Utils.getTunnelInstance();
        try {
            tunnel.createStreamUploadSession(projectName, tableName, new PartitionSpec("pt='a'"));
            Assert.fail("A Tunnel Exception should be thrown");
        } catch (TunnelException e) {
            Assert.assertThat(e.getMessage(), CoreMatchers.containsString("NoSuchPartition"));
        }

        Utils.dropTableIfExists(tableName);
    }
}
