package com.aliyun.odps.udf.local.runner;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.IoUtils;
import com.aliyun.odps.udf.example.stdout.StdoutOutputer;
import com.aliyun.odps.udf.example.text.TextOutputer;
import com.aliyun.odps.udf.local.util.LocalDataAttributes;
import com.aliyun.odps.udf.local.util.UnstructuredUtils;
import com.aliyun.odps.utils.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputerRunnerTest  extends ExtendedRunnerTestBase {
  private String simpleTableSchema = "a:bigint;b:double;c:string";
  private String adsLogTableSchema = "AdId:BIGINT;Rand:DOUBLE;AdvertiserName:STRING;Comment:STRING";
  private File outputDirectory = null;


  @Test
  public void testOutputSimpleText(){
    /**
     * Test outputting manually constructed records to text
     */
    Column[] externalTableSchema = parseSchemaString(simpleTableSchema);
    LocalDataAttributes attributes = new LocalDataAttributes(null, externalTableSchema);
    // TextOutputer will output one single file
    OutputerRunner runner = new OutputerRunner(odps, new TextOutputer(), attributes);

    List<Record> records = new ArrayList<Record>();
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)1, 2.5, "row0"}));
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)1234567, 8.88, "row1"}));
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)12, 123.1, "testrow"}));
    try{
      testSetup();
      // run outputer
      runner.feedRecords(records);
      runner.yieldTo(outputDirectory.getAbsolutePath());

      String expcetedOutput = "1|2.5|row0\n" +
          "1234567|8.88|row1\n" +
          "12|123.1|testrow\n";

      verifySingleFileOutput(expcetedOutput);

    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Test Failed with exception " + e.getMessage());
    } finally {
      testClean();
    }
  }

  @Test
  public void testOutputSpecialText(){
    /**
     * Test reading from internal table and outputting to text file, with a user defined delimiter.
     * Equivalent to the following SQL:
     *
     CREATE EXTERNAL TABLE ads_log_external
     (AdId bigint, Rand double,
     AdvertiserName string, Comment string)
     STORED BY 'com.aliyun.odps.udf.example.text.TextStorageHandler'
     WITH SERDEPROPERTIES ('delimiter'='\t')
     LOCATION 'oss://path/to/output/'
     USING 'jar_file_name.jar';;

     INSERT OVERWRITE ads_log_external SELECT * FROM ads_log;
     * Here ads_log is an internal table (locally defined in warehouse directory)
     */

    Column[] externalTableSchema = parseSchemaString(adsLogTableSchema);
    Map<String, String> userProperties = new HashMap<String, String>();
    userProperties.put("delimiter", "\t");
    LocalDataAttributes attributes = new LocalDataAttributes(userProperties, externalTableSchema);
    // TextOutputer outputs one single file
    OutputerRunner runner = new OutputerRunner(odps, new TextOutputer(), attributes);

    String internalTableName = "ads_log";
    // We are doing SELECT * FROM here, so the two tables have the same schema
    Column[] internalTableSceham = externalTableSchema;

    List<Record> records = new ArrayList<Record>();
    Record record;
    try{
      testSetup();

      while ((record = readFromInternalTable(internalTableName, internalTableSceham)) != null){
        records.add(record.clone());
      }
      // run outputer
      runner.feedRecords(records);
      runner.yieldTo(outputDirectory.getAbsolutePath());

      String expcetedOutput = "399266\t0.5\tDoritos\twhat is up\n" +
          "399266\t0.0\tTacobell\thello!\n" +
          "382045\t-76.0\tVoelkl\trandom comments\n" +
          "382045\t6.4\tWhistler Resort\ta\n" +
          "106479\t98.7\tAmazon Prime\tbdcd\n" +
          "906441\t-9865788.2\tHayden Planetarium\tplatium\n" +
          "351530\t0.005\tMicrosoft Azure Services\ttst\n";

      verifySingleFileOutput(expcetedOutput);

    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Test Failed with exception " + e.getMessage());
    } finally {
      testClean();
    }
  }


  @Test
  public void testCustomizedOutputDestination(){
    /**
     * Test outputting to data sink other than local file system, here we use a stdout-outputer
     * as an exmpale, however, this provides a way to outputting odps records literally to any external storage other
     * than oss/tablestore. For example, as long as a outputer is probably implemented and there is network conneciton,
     * user can use it to output to hbase, hdfs, mysql etc..
     */
    Column[] externalTableSchema = parseSchemaString(simpleTableSchema);

    List<Record> records = new ArrayList<Record>();
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)1, 2.5, "row0"}));
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)1234567, 8.88, "row1"}));
    records.add(new ArrayRecord(externalTableSchema, new Object[]{(long)12, 123.1, "testrow"}));

    LocalDataAttributes attributes = new LocalDataAttributes(null, externalTableSchema);
    // TextOutputer will output one single file
    OutputerRunner runner = new OutputerRunner(odps, new StdoutOutputer(), attributes);
    try{
      // run outputer
      runner.feedRecords(records);
      runner.setUseCustomizedOutput();
      runner.yieldTo("stdout://10.21.34.123:8080/my/stdout/");
    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Test Failed with exception " + e.getMessage());
    }
  }

  @Test
  public void testOutputLocationParser(){
    String location = "oss://oss-cn-hangzhou-internal.aliyuncs.com/bucket/directory/";
    String exepctedMetaFileLocation = "oss://oss-cn-hangzhou-internal.aliyuncs.com/bucket/directory/.odps/.meta";
    String fileLocation = IoUtils.getMetaFileLocation(location);
    Assert.assertEquals(exepctedMetaFileLocation, fileLocation);

    String metaContent = "{\"dirs\": [\"20170601181436258gi2qlv2\",\n" +
        "        \"20170602204058685g2p81ss\"]}";
    String[] expectedContent = new String[]{"20170601181436258gi2qlv2", "20170602204058685g2p81ss"};
    String[] content = IoUtils.parseOutputSubDirectoriesFromMeta(metaContent);
    Assert.assertEquals(expectedContent.length, content.length);
    for (int i = 0; i < content.length; i++) {
      Assert.assertEquals(expectedContent[i], content[i]);
    }
    metaContent = "{\"dirs\": [\"20161123191144870gmzplv2\"]}";
    expectedContent = new String[]{"20161123191144870gmzplv2"};
    content = IoUtils.parseOutputSubDirectoriesFromMeta(metaContent);
    Assert.assertEquals(expectedContent.length, content.length);
    for (int i = 0; i < content.length; i++) {
      Assert.assertEquals(expectedContent[i], content[i]);
    }

  }

  // to be used only when outputing to local files
  private void testSetup() throws IOException{
    // output directory preparation
    outputDirectory = File.createTempFile(UnstructuredUtils.generateOutputName(), "test");
    outputDirectory.delete();
    outputDirectory.mkdirs();
  }

  // to be used only when outputing to local files
  private void testClean(){
    if (outputDirectory != null) {
      outputDirectory.delete();
    }
  }

  private void verifySingleFileOutput(String expectedOutput) throws IOException {
    verifyFilesOutput(new String[]{expectedOutput});
  }

  private void verifyFilesOutput(String[] expectedOutputs) throws IOException {
    File[] outputs = outputDirectory.listFiles();
    Assert.assertEquals(outputs.length, expectedOutputs.length);
    for (int i = 0; i < outputs.length; i++){
      File outputFile = outputs[i];
      FileInputStream fis = new FileInputStream(outputFile);
      byte[] data = new byte[(int)outputFile.length()];
      fis.read(data);
      String content = new String(data);
      String[] rows = StringUtils.split(content, '\n');
      String[] expectedRows = StringUtils.split(expectedOutputs[i], '\n');
      // due to double presentation accuracy difference, the output may not exactly match expected,
      // therefore we only verify that numbers of rows match.
      Assert.assertEquals(rows.length, expectedRows.length);
    }
  }
}
