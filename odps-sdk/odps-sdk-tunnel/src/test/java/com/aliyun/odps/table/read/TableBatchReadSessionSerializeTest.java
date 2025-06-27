package com.aliyun.odps.table.read;

import java.util.Arrays;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.impl.batch.TableBatchReadSessionImpl;
import com.aliyun.odps.table.read.impl.incremental.TableIncrementalReadSessionImpl;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.tunnel.OdpsTunnelTestUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TableBatchReadSessionSerializeTest {


    @Test
    public void testSerializeBatchReadSession() throws Exception {
        Odps odps = OdpsTestUtils.newDefaultOdps();
        String tableName = "test_serialize_read_session";
        OdpsTestUtils.createTableForTest(tableName);

        Credentials credentials = Credentials.newBuilder()
            .withAccount(odps.getAccount())
            .withAppAccount(odps.getAppAccount())
            .build();

        EnvironmentSettings settings = EnvironmentSettings.newBuilder()
            .inLocalMode()
            .withCredentials(credentials)
            .withServiceEndpoint(odps.getEndpoint())
            .withTunnelEndpoint(OdpsTunnelTestUtils.getProperty("default.tunnel.endpoint"))
            .build();

        TableReadSessionBuilder readBuilder = new TableReadSessionBuilder();
        TableBatchReadSession session = readBuilder.identifier(
                TableIdentifier.of(odps.getDefaultProject(), tableName))
            .requiredDataColumns(Arrays.asList("c1", "c2"))
            .withSettings(settings)
            .buildBatchReadSession();

        InputSplit[] allSplits = session.getInputSplitAssigner().getAllSplits();

        String serilized = session.toJson();

        System.out.println(serilized);

        TableBatchReadSession
            deserialized =
            new TableReadSessionBuilder().fromJson(serilized).buildBatchReadSession();

        Assert.assertEquals(deserialized.getClass().getName(), TableBatchReadSessionImpl.class.getName());

        SplitReader<VectorSchemaRoot> arrowReader = deserialized.createArrowReader(allSplits[0],
                                                                                   ReaderOptions.newBuilder()
                                                                                       .withSettings(settings)
                                                                                       .build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot root = arrowReader.get();
            System.out.println(root.contentToTSVString());
        }
    }


    @Test
    public void testSerializeIncrementalReadSession() throws Exception {
        Odps odps = OdpsTestUtils.newDefaultOdps();
        String tableName = "test_serialize_read_session_delta";
        OdpsTestUtils.createDeltaTableForTest(odps, tableName);

        Credentials credentials = Credentials.newBuilder()
            .withAccount(odps.getAccount())
            .withAppAccount(odps.getAppAccount())
            .build();

        EnvironmentSettings settings = EnvironmentSettings.newBuilder()
            .inLocalMode()
            .withCredentials(credentials)
            .withServiceEndpoint(odps.getEndpoint())
            .withTunnelEndpoint(OdpsTunnelTestUtils.getProperty("default.tunnel.endpoint"))
            .build();

        TableReadSessionBuilder readBuilder = new TableReadSessionBuilder();
        TableBatchReadSession session = readBuilder.identifier(
                TableIdentifier.of(odps.getDefaultProject(), tableName))
            .requiredDataColumns(Arrays.asList("c1", "c2"))
            .withSettings(settings)
            .withIncrementalOptions(IncrementalOptions.newBuilder().startVersion(0L).build())
            .buildIncrementalReadSession();

        InputSplit[] allSplits = session.getInputSplitAssigner().getAllSplits();

        String serilized = session.toJson();

        System.out.println(serilized);

        TableBatchReadSession
            deserialized =
            new TableReadSessionBuilder().fromJson(serilized).buildIncrementalReadSession();

        Assert.assertEquals(deserialized.getClass().getName(), TableIncrementalReadSessionImpl.class.getName());

        SplitReader<VectorSchemaRoot> arrowReader = deserialized.createArrowReader(allSplits[0],
                                                                                   ReaderOptions.newBuilder()
                                                                                       .withSettings(settings)
                                                                                       .build());
        while (arrowReader.hasNext()) {
            VectorSchemaRoot root = arrowReader.get();
            System.out.println(root.contentToTSVString());
        }
    }
}
