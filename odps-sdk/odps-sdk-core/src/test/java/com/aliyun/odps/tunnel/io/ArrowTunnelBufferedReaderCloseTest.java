package com.aliyun.odps.tunnel.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfoFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

/**
 * Arrow 批次读到一半抛错时，ArrowTunnelBufferedReader 有没有把临时 reader 收尾（the original regression）。
 *
 * <p>全部离线：不碰 endpoint、凭据或 project，也不申请任何 arrow buffer —— 替身交出的
 * {@link VectorSchemaRoot} 是零向量的空壳，本项只关心 close() 被调了几次、按什么顺序调。替身从包级接缝
 * {@link ArrowTunnelBufferedReader#openBatchReader(long, long)} 注入。原来 openReader() 里没有
 * try/finally，一次 read() 抛错（Arrow IPC 流被截断、消息头反序列化失败都是这个形状）就把
 * {@link ArrowTunnelRecordReader#close()} 整段跳过，而 inputStream.close() 与 connection.disconnect()
 * 都挂在那里；调用方即使规矩地 close() 也补不回来（它手上只有 rootBuffer）。
 */
public class ArrowTunnelBufferedReaderCloseTest {

  private static final List<Column> COLUMNS =
      Collections.singletonList(new Column("c", TypeInfoFactory.STRING));

  private static final Schema EMPTY_SCHEMA = new Schema(Collections.emptyList());

  /**
   * 批次 reader 替身：前 rootsBefore 次 read() 交出一个 root，之后要么抛错（readFailure 非 null）要么
   * 返回 null（正常读完）。bytesRead() 按每个 root 的增量累计，用于钉住计数语义。记录调用顺序、close()
   * 次数，并可让第一次 close() 失败。
   */
  private static final class StubArrowReader implements ArrowRecordReader {

    /** 每个交出的 root 为本批新增的字节数，故意不等长：增量与累计写混了就会被下面的断言抓到。 */
    private static final long[] ROOT_BYTES = {10L, 20L, 30L};

    private final int rootsBefore;
    private final IOException readFailure;
    private final List<VectorSchemaRoot> handedOut = new ArrayList<>();
    private final List<String> calls = new ArrayList<>();

    private IOException firstCloseFailure;
    private int emitted;
    private int closeCount;
    private long cumulativeBytes;

    private StubArrowReader(int rootsBefore, IOException readFailure) {
      this.rootsBefore = rootsBefore;
      this.readFailure = readFailure;
    }

    static StubArrowReader failsOnReadAfter(int rootsBefore, String message) {
      return new StubArrowReader(rootsBefore, new IOException(message));
    }

    static StubArrowReader endsAfter(int rootsBefore) {
      return new StubArrowReader(rootsBefore, null);
    }

    StubArrowReader failFirstClose(String message) {
      this.firstCloseFailure = new IOException(message);
      return this;
    }

    int closeCount() {
      return closeCount;
    }

    @Override
    public VectorSchemaRoot read() throws IOException {
      calls.add("read");
      if (emitted < rootsBefore) {
        VectorSchemaRoot root =
            new VectorSchemaRoot(EMPTY_SCHEMA, Collections.<FieldVector>emptyList(), 0);
        handedOut.add(root);
        emitted++;
        cumulativeBytes += ROOT_BYTES[(emitted - 1) % ROOT_BYTES.length];
        return root;
      }
      if (readFailure != null) {
        throw readFailure;
      }
      return null;
    }

    @Override
    public long bytesRead() {
      return cumulativeBytes;
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

  /** 从接缝注入替身的 ArrowTunnelBufferedReader；构造只存字段，session 传 null 也不会发请求。 */
  private static final class SeamedReader extends ArrowTunnelBufferedReader {

    private final ArrowRecordReader batchReader;
    private final List<String> seamCalls = new ArrayList<>();

    SeamedReader(ArrowRecordReader batchReader, long start, long count, long batchSize) {
      super(start, count, batchSize, COLUMNS, (TableTunnel.DownloadSession) null, null,
          new CompressOption(), false);
      this.batchReader = batchReader;
    }

    @Override
    ArrowRecordReader openBatchReader(long readerStart, long readerCount) {
      seamCalls.add(readerStart + "+" + readerCount);
      return batchReader;
    }
  }

  @Test
  public void midBatchReadFailureStillClosesTheBatchReader() throws IOException {
    StubArrowReader stub = StubArrowReader.failsOnReadAfter(1, "arrow stream truncated mid-batch");
    SeamedReader reader = new SeamedReader(stub, 0, 10, 5);

    try {
      reader.read();
      fail("批次中途的读错误要原样抛给调用方");
    } catch (IOException expected) {
      assertEquals("arrow stream truncated mid-batch", expected.getMessage());
    }

    assertEquals("抛错路径也必须关掉临时 reader：inputStream.close() 与 connection.disconnect() 都挂在它身上",
        1, stub.closeCount());
    assertEquals(Arrays.asList("read", "read", "close#1"), stub.calls);
  }

  @Test
  public void successfulBatchClosesReaderOnceInUnchangedOrder() throws IOException {
    StubArrowReader stub = StubArrowReader.endsAfter(3);
    SeamedReader reader = new SeamedReader(stub, 0, 3, 3);

    // 先读再比引用：assertSame(expected, actual) 的实参从左到右求值，把 handedOut.get(i) 写在
    // reader.read() 之前会在替身还没吐出 root 时取空列表（第一轮就是这么挂的，是测试自己的缺陷）。
    VectorSchemaRoot root1 = reader.read();
    assertEquals("读到第 1 个 root 后累计字节数", 10L, reader.bytesRead());
    VectorSchemaRoot root2 = reader.read();
    assertEquals(30L, reader.bytesRead());
    VectorSchemaRoot root3 = reader.read();
    assertEquals(60L, reader.bytesRead());
    assertSame(stub.handedOut.get(0), root1);
    assertSame(stub.handedOut.get(1), root2);
    assertSame(stub.handedOut.get(2), root3);
    assertNull(reader.read());

    assertEquals("一批只按 (start, min(count, batchSize)) 开一次 reader", Arrays.asList("0+3"),
        reader.seamCalls);
    assertEquals("成功路径的关闭时机一字不变：读完 -> close，且只关一次",
        Arrays.asList("read", "read", "read", "read", "close#1"), stub.calls);
  }

  @Test
  public void closeFailureDoesNotMaskTheReadFailureAndCallerCloseRetries() throws IOException {
    StubArrowReader stub = StubArrowReader.failsOnReadAfter(1, "bad arrow message header")
        .failFirstClose("disconnect failed");
    SeamedReader reader = new SeamedReader(stub, 0, 10, 5);

    try {
      reader.read();
      fail("收尾失败不能顶掉让本批失败的原始异常");
    } catch (IOException expected) {
      assertEquals("bad arrow message header", expected.getMessage());
      assertEquals("原始异常的类型与 message 不变，只多一条 suppressed", 1,
          expected.getSuppressed().length);
      assertEquals("收尾失败不静默丢弃：挂在原始异常上交给调用方", "disconnect failed",
          expected.getSuppressed()[0].getMessage());
    }
    assertEquals(1, stub.closeCount());

    reader.close();
    assertEquals("调用方 close() 的兜底：没关成功的批次再试一次", 2, stub.closeCount());
  }

  @Test
  public void callerCloseDoesNotReCloseASettledReader() throws IOException {
    StubArrowReader stub = StubArrowReader.endsAfter(1);
    SeamedReader reader = new SeamedReader(stub, 0, 1, 1);

    VectorSchemaRoot root = reader.read();
    assertSame(stub.handedOut.get(0), root);
    assertNull(reader.read());
    reader.close();
    reader.close();

    assertEquals("已经收尾过的批次不再二次 close", 1, stub.closeCount());
  }

  @Test
  public void callerCloseReleasesUndeliveredArrowBuffers() throws IOException {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      VarCharVector vector = new VarCharVector("value", allocator);
      vector.allocateNew();
      VectorSchemaRoot root = VectorSchemaRoot.of(vector);
      ArrowRecordReader batch = new ArrowRecordReader() {
        private int reads;

        @Override
        public VectorSchemaRoot read() throws IOException {
          if (reads++ == 0) {
            return root;
          }
          throw new IOException("truncated Arrow batch");
        }

        @Override
        public long bytesRead() {
          return 1;
        }

        @Override
        public void close() {
        }
      };
      SeamedReader reader = new SeamedReader(batch, 0, 2, 2);
      try {
        reader.read();
        fail("expected the second read to fail");
      } catch (IOException expected) {
        assertEquals("truncated Arrow batch", expected.getMessage());
      }
      assertTrue(allocator.getAllocatedMemory() > 0);
      reader.close();
      assertEquals("buffered root must release its Arrow allocation", 0,
          allocator.getAllocatedMemory());
    }
  }

  /**
   * 次生事实，本项不改语义：游标推进排在循环之后，批次中途抛错时 start/count 不前移，缓冲里半批的 root
   * 仍会被交出去，之后同一批会被重开一次。是否收紧另判，先由用例钉住"修复前后行为一致"。
   */
  @Test
  public void readFailureLeavesCursorAndBufferedRootsUnchanged() throws IOException {
    StubArrowReader stub = StubArrowReader.failsOnReadAfter(1, "arrow stream truncated mid-batch");
    SeamedReader reader = new SeamedReader(stub, 0, 10, 5);

    try {
      reader.read();
      fail("第一次读要抛错");
    } catch (IOException expected) {
      assertEquals("arrow stream truncated mid-batch", expected.getMessage());
    }
    VectorSchemaRoot buffered = reader.read();
    assertSame("半批已入缓冲的 root 仍按原行为交出去", stub.handedOut.get(0), buffered);
    try {
      reader.read();
      fail("缓冲排空后重开同一批，再抛错");
    } catch (IOException expected) {
      assertEquals("arrow stream truncated mid-batch", expected.getMessage());
    }

    assertEquals("游标不前进：两次开批次的参数完全相同", Arrays.asList("0+5", "0+5"), reader.seamCalls);
  }

  @Test
  public void defaultSeamStillRefusesToOpenWithoutSession() throws Exception {
    ArrowTunnelBufferedReader reader = new ArrowTunnelBufferedReader(0, 10, 5, COLUMNS,
        (TableTunnel.DownloadSession) null, null, new CompressOption(), false);
    try {
      reader.openBatchReader(0, 5);
      fail("session 为 null 时必须照旧拒绝开批次");
    } catch (IllegalArgumentException expected) {
      assertEquals("Cannot create record reader if session is null.", expected.getMessage());
    }
  }
}
