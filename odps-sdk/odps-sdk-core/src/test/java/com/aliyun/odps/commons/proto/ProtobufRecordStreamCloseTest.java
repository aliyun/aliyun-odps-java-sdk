package com.aliyun.odps.commons.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm;
import com.aliyun.odps.type.TypeInfoFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * 压缩档读到一半就 close() 时，包在 bin 外面那层解压流有没有被关到（the original regression 的第二处）。
 *
 * <p>改之前 ProtobufRecordStreamReader 的构造把 bin 包进 InflaterInputStream /
 * SnappyFramedInputStream / LZ4FrameInputStream / ZstdInputStream，但那层包装是匿名对象，close()
 * 只关到 bin —— 解压上下文（Inflater 的 native 上下文、frame 状态）只能等 finalizer 回收。
 *
 * <p>可观测性的边界，说清楚免得把没测的写成测了：底层流是 BufferedInputStream 包的，它的 close 是一次性的，
 * 所以"底层流被关到两次"这类计数判据不成立；CodedInputStream 又自带缓冲，close 后先出来的还是缓冲区里的记录。
 * 因此这里能钉住的只有两件事：① 早关（帧没读完）时 close() 自己不许抛，四种压缩算法加 RAW 都测；
 * ② zlib 这条上，close 之后继续读必须以失败收尾 —— 修复前解压层还活着，剩下的记录和流尾照常解完，
 * 读到一个干净的 EOF（null）就返回了，看起来跟"顺利读完"一模一样。snappy/lz4/zstd 在两个版本上都是
 * IOException（它们的实现在 close 后 refill 就报错），分不出差别，这三条只按代码事实说明：那层包装流以前没人关。
 */
public class ProtobufRecordStreamCloseTest {

  private static final Column[] COLUMNS = new Column[] {new Column("c", TypeInfoFactory.STRING)};

  private static final List<Column> COLUMN_LIST =
      Collections.unmodifiableList(Arrays.asList(COLUMNS));

  private static final CompressAlgorithm[] CODECS = new CompressAlgorithm[] {
      CompressAlgorithm.ODPS_ZLIB, CompressAlgorithm.ODPS_SNAPPY,
      CompressAlgorithm.ODPS_LZ4_FRAME, CompressAlgorithm.ODPS_ZSTD, CompressAlgorithm.ODPS_RAW};

  private static TableSchema schema() {
    TableSchema schema = new TableSchema();
    schema.addColumn(COLUMNS[0]);
    return schema;
  }

  private static byte[] writeRecords(CompressOption option, int records) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ProtobufRecordStreamWriter writer = new ProtobufRecordStreamWriter(schema(), bytes, option);
    for (int i = 1; i <= records; i++) {
      ArrayRecord record = new ArrayRecord(COLUMNS);
      record.set(0, "row-" + i);
      writer.write(record);
    }
    // writer.close() 写流尾（META_COUNT + 校验和）并收尾压缩帧
    writer.close();
    return bytes.toByteArray();
  }

  private static ProtobufRecordStreamReader reader(CompressOption option, int records)
      throws IOException {
    return new ProtobufRecordStreamReader(schema(), COLUMN_LIST,
        new ByteArrayInputStream(writeRecords(option, records)), option);
  }

  private static String text(Record record) {
    Object value = record.get(0);
    return value instanceof byte[]
        ? new String((byte[]) value, StandardCharsets.UTF_8) : String.valueOf(value);
  }

  /**
   * 兼容性护栏：读到第一条就关（用户中途放弃一批下载是常态），五种算法都不许在 close() 里抛。
   * 这条修复前后都绿，作用是挡住"为了能关解压层而让 close 开始报错"这类改法。
   */
  @Test
  public void closingEarlyNeverThrows() throws IOException {
    for (CompressAlgorithm codec : CODECS) {
      CompressOption option = new CompressOption(codec, 1, 0);
      ProtobufRecordStreamReader reader = reader(option, 3);
      assertEquals("压缩流应能读出第一条", "row-1", text(reader.read()));
      reader.close();
    }
  }

  /**
   * 压缩层收尾的证据：zlib 档 close 之后继续把剩下的读干净，必须失败，不许出现"顺利读到 EOF"。
   * 修复前这里能读出 row-2、row-3 然后正常返回 null —— 解压上下文还活着，剩下的帧照样能解。
   */
  @Test
  public void zlibReaderDoesNotFinishTheStreamAfterClose() throws IOException {
    ProtobufRecordStreamReader reader =
        reader(new CompressOption(CompressAlgorithm.ODPS_ZLIB, 1, 0), 3);

    assertEquals("row-1", text(reader.read()));
    reader.close();

    int drained = 0;
    try {
      for (Record next = reader.read(); next != null; next = reader.read()) {
        drained++;
      }
      fail("close 之后不该还能把剩下的记录和流尾顺利解完（解压层没关）；实际又解出 " + drained + " 条");
    } catch (IOException expected) {
      assertTrue("异常应当来自被关闭的解压流，实际是 " + expected.getMessage(),
          expected.getMessage() != null && expected.getMessage().toLowerCase().contains("closed"));
    }
  }
}
