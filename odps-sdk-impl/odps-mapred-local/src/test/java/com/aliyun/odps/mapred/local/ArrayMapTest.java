package com.aliyun.odps.mapred.local;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.unittest.KeyValue;
import com.aliyun.odps.mapred.unittest.MRUnitTest;
import com.aliyun.odps.mapred.unittest.MapUTContext;
import com.aliyun.odps.mapred.unittest.ReduceUTContext;
import com.aliyun.odps.mapred.unittest.TaskOutput;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ArrayMapTest extends MRUnitTest {
  private final static String INPUT_SCHEMA = "a_array:array<string>";
  private final static String OUTPUT_SCHEMA = "a_map:map<string, bigint>";
  private JobConf job;

  public static class TokenizerMapper extends MapperBase {
    Record word;
    Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[] {1L});
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      ArrayWritable arrayWritable = (ArrayWritable) record.get(0);
      if (arrayWritable != null) {
        for (Writable writable : arrayWritable.get()) {
          word.set(new Object[]{writable});
          context.write(word, one);
        }
      }
    }
  }

  public static class SumReducer extends ReducerBase {
    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += val.getBigint(0);
      }
      MapWritable mapWritable = new MapWritable();
      mapWritable.put(new Text(key.getString(0)), new LongWritable(count));
      result.set(0, mapWritable);
      context.write(result);
    }
  }

  public ArrayMapTest() {
    job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));
    InputUtils.addTable(TableInfo.builder().tableName("complex_array").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("complex_map").build(), job);
  }

  @Test
  public void testMap() throws IOException, ClassNotFoundException, InterruptedException {
    MapUTContext mapContext = new MapUTContext();
    mapContext.setInputSchema(INPUT_SCHEMA);
    mapContext.setOutputSchema(OUTPUT_SCHEMA, job);

    Record record = mapContext.createInputRecord();
    Writable[] w1s = new Writable[2];
    w1s[0] = new Text("a");
    w1s[1] = new Text("b");
    record.set(new Writable[] {new ArrayWritable(Text.class, w1s)});
    mapContext.addInputRecord(record);

    record = mapContext.createInputRecord();
    Writable[] w2s = new Writable[2];
    w2s[0] = new Text("b");
    w2s[1] = new Text("c");
    record.set(new Writable[] {new ArrayWritable(Text.class, w2s)});
    mapContext.addInputRecord(record);

    TaskOutput output = runMapper(job, mapContext);

    List<KeyValue<Record, Record>> kvs = output.getOutputKeyValues();
    Assert.assertEquals(4, kvs.size());
    Assert.assertEquals(new KeyValue<>("a", new Long(1)),
        new KeyValue<>((String) (kvs.get(0).getKey().get(0)), (Long) (kvs.get(0).getValue().get(0))));
    Assert.assertEquals(new KeyValue<>(new String("b"), new Long(1)),
        new KeyValue<>((String) (kvs.get(1).getKey().get(0)), (Long) (kvs.get(1).getValue().get(0))));
    Assert.assertEquals(new KeyValue<>(new String("b"), new Long(1)),
        new KeyValue<>((String) (kvs.get(2).getKey().get(0)), (Long) (kvs.get(2).getValue().get(0))));
    Assert.assertEquals(new KeyValue<>(new String("c"), new Long(1)),
        new KeyValue<>((String) (kvs.get(3).getKey().get(0)), (Long) (kvs.get(3).getValue().get(0))));
  }

  @Test
  public void testReduce() throws IOException, ClassNotFoundException, InterruptedException {
    ReduceUTContext context = new ReduceUTContext();
    context.setOutputSchema(OUTPUT_SCHEMA,  job);
    Record key = context.createInputKeyRecord(job);
    Record value = context.createInputValueRecord(job);
    key.set(0, "a");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "b");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "b");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);
    key.set(0, "c");
    value.set(0, new Long(1));
    context.addInputKeyValue(key, value);

    TaskOutput output = runReducer(job, context);

    List<Record> records = output.getOutputRecords(false);
    Assert.assertEquals(3, records.size());
    Assert.assertEquals(new LongWritable(1), ((MapWritable) records.get(0).get("a_map")).get(new Text("a")));
    Assert.assertEquals(new LongWritable(2), ((MapWritable) records.get(1).get("a_map")).get(new Text("b")));
    Assert.assertEquals(new LongWritable(1), ((MapWritable) records.get(2).get("a_map")).get(new Text("c")));
  }

}
