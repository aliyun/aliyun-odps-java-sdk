package com.aliyun.odps.data.converter;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class FormatStructTest {

    static OdpsRecordConverter odpsRecordConverter;

    static {
      odpsRecordConverter = OdpsRecordConverter.builder()
          .setStrictMode(false)
          .enableParseNull()
          .build();
    }

    @Test
    public void testListStruct() throws Exception {
      Odps odps = OdpsTestUtils.newDefaultOdps();
      SQLExecutor sqlExecutor = SQLExecutorBuilder.builder().odps(odps)
          .enableCommandApi(true)
          .useInstanceTunnel(true)
          .build();
      sqlExecutor.run("desc extended test;", null);

      Record record = sqlExecutor.getResult().get(0);

      String[] strings = odpsRecordConverter.formatRecord(record);
      for (String string : strings) {
        System.out.println(string);
      }
    }



}
