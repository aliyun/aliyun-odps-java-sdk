package com.aliyun.odps.tunnel.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelMetrics;
import com.aliyun.odps.type.TypeInfoFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * 批次读到一半抛错时，TunnelBufferedReader 有没有把临时 reader 收尾（the original regression）。
 *
 * <p>全部离线：不碰 endpoint、凭据或 project。替身从包级接缝 {@link
 * TunnelBufferedReader#openBatchReader(long, long)} 注入，只观察两件事 —— close() 被调了几次、
 * 按什么顺序调。原来 openReader() 里没有 try/finally，一次 read() 抛错（流被截断、校验和不对、
 * protobuf 帧不完整都是实测过的形状）就把 RawTunnelRecordReader.close() 里的 conn.disconnect()
 * 整个跳过，而调用方即使规矩地 close() 也补不回来（它手上只有 recordBuffer）。
 */
public class TunnelBufferedReaderCloseTest {

  private static final List<Column> COLUMNS =
      Collections.singletonList(new Column("c", TypeInfoFactory.STRING));

  /**
   * 批次 reader 替身：前 recordsBefore 次 read() 返回记录，之后要么抛错（failure 非 null）要么返回
   * null（正常读完）。记录调用顺序、close() 次数，并可让第一次 close() 失败。
   */
  private static final class StubBatchReader implements BatchRecordReader {

    private final int recordsBefore;
    private final IOException readFailure;
    private final List<String> calls = new ArrayList<>();
    private IOException firstCloseFailure;
    private int emitted;
    private int closeCount;

    private StubBatchReader(int recordsBefore, IOException readFailure) {
      this.recordsBefore = recordsBefore;
      this.readFailure = readFailure;
    }

    static StubBatchReader failsOnReadAfter(int recordsBefore, String message) {
      return new StubBatchReader(recordsBefore, new IOException(message));
    }

    static StubBatchReader endsAfter(int recordsBefore) {
      return new StubBatchReader(recordsBefore, null);
    }

    StubBatchReader failFirstClose(String message) {
      this.firstCloseFailure = new IOException(message);
      return this;
    }

    int closeCount() {
      return closeCount;
    }

    @Override
    public Record read() throws IOException {
      calls.add("read");
      if (emitted++ < recordsBefore) {
        ArrayRecord record = new ArrayRecord(COLUMNS.toArray(new Column[0]));
        record.set(0, "row-" + emitted);
        return record;
      }
      if (readFailure != null) {
        throw readFailure;
      }
      return null;
    }

    @Override
    public void setTransform(boolean shouldTransform) {
      calls.add("setTransform");
    }

    @Override
    public void setRequireStreamFooter(boolean requireStreamFooter) {
      calls.add("setRequireStreamFooter");
    }

    @Override
    public TunnelMetrics getMetrics() {
      calls.add("getMetrics");
      return new TunnelMetrics();
    }

    @Override
    public void close() throws IOException {
      closeCount++;
      calls.add("close#" + closeCount);
      if (closeCount == 1 && firstCloseFailure != null) {
        IOException toThrow = firstCloseFailure;
        firstCloseFailure = null;
        throw toThrow;
      }
    }
  }

  /** 从接缝注入替身的 TunnelBufferedReader；构造只存字段，session 传 null 也不会发请求。 */
  private static final class SeamedReader extends TunnelBufferedReader {

    private final BatchRecordReader batchReader;
    private final List<String> seamCalls = new ArrayList<>();

    SeamedReader(BatchRecordReader batchReader, long start, long count, long batchSize) {
      super(start, count, batchSize, 0L, COLUMNS, new CompressOption(),
          (TableTunnel.DownloadSession) null, false);
      this.batchReader = batchReader;
    }

    @Override
    BatchRecordReader openBatchReader(long readerStart, long readerCount) {
      seamCalls.add(readerStart + "+" + readerCount);
      return batchReader;
    }
  }

  @Test
  public void midBatchReadFailureStillClosesTheBatchReader() throws IOException {
    StubBatchReader stub = StubBatchReader.failsOnReadAfter(1, "stream truncated mid-batch");
    SeamedReader reader = new SeamedReader(stub, 0, 10, 5);

    try {
      reader.read();
      fail("批次中途的读错误要原样抛给调用方");
    } catch (IOException expected) {
      assertEquals("stream truncated mid-batch", expected.getMessage());
    }

    assertEquals("抛错路径也必须关掉临时 reader：conn.disconnect() 与解压层收尾都挂在它身上",
        1, stub.closeCount());
    // 失败批次不累加 metrics：半截统计量本来就不代表一个完整批次，这一点与修复前一致
    assertEquals(Arrays.asList("setTransform", "setRequireStreamFooter", "read", "read", "close#1"),
        stub.calls);
  }

  @Test
  public void successfulBatchClosesReaderOnceInUnchangedOrder() throws IOException {
    StubBatchReader stub = StubBatchReader.endsAfter(3);
    SeamedReader reader = new SeamedReader(stub, 0, 3, 3);

    for (int i = 1; i <= 3; i++) {
      assertEquals("row-" + i, reader.read().get(0));
    }
    assertNull(reader.read());

    assertEquals(Arrays.asList("0+3"), reader.seamCalls);
    assertEquals("成功路径的关闭时机一字不变：读完 -> close -> 取 metrics，且只关一次",
        Arrays.asList("setTransform", "setRequireStreamFooter", "read", "read", "read", "read",
            "close#1", "getMetrics"), stub.calls);
  }

  @Test
  public void closeFailureDoesNotMaskTheReadFailureAndCallerCloseRetries() throws IOException {
    StubBatchReader stub =
        StubBatchReader.failsOnReadAfter(1, "bad record frame").failFirstClose("disconnect failed");
    SeamedReader reader = new SeamedReader(stub, 0, 10, 5);

    try {
      reader.read();
      fail("收尾失败不能顶掉让本批失败的原始异常");
    } catch (IOException expected) {
      assertEquals("bad record frame", expected.getMessage());
    }
    assertEquals(1, stub.closeCount());

    reader.close();
    assertEquals("调用方 close() 的兜底：没关成功的批次再试一次", 2, stub.closeCount());
  }

  @Test
  public void callerCloseDoesNotReCloseASettledReader() throws IOException {
    StubBatchReader stub = StubBatchReader.endsAfter(1);
    SeamedReader reader = new SeamedReader(stub, 0, 1, 1);

    assertEquals("row-1", reader.read().get(0));
    assertNull(reader.read());
    reader.close();
    reader.close();

    assertEquals("已经收尾过的批次不再二次 close，metrics 也不会重复累加", 1, stub.closeCount());
  }

  @Test
  public void defaultSeamStillRefusesToOpenWithoutSession() throws Exception {
    TunnelBufferedReader reader = new TunnelBufferedReader(0, 10, 5, 0L, COLUMNS,
        new CompressOption(), (TableTunnel.DownloadSession) null, false);
    try {
      reader.openBatchReader(0, 5);
      fail("两个 session 都是 null 时必须照旧拒绝开批次");
    } catch (IllegalArgumentException expected) {
      assertEquals("Cannot create record reader if session is null.", expected.getMessage());
    }
  }
}
