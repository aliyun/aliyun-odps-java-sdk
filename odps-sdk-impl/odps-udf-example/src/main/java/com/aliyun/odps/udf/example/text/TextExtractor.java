package com.aliyun.odps.udf.example.text;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.*;
import com.aliyun.odps.utils.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * Text extractor that extract schematized records from formatted plain-text(csv, tsv etc.)
 **/
public class TextExtractor extends Extractor {

  private InputStreamSet inputs;
  private char delimiterChar;
  private char linebreakChar;
  private DataAttributes attributes;
  private BufferedReader currentReader;
  private boolean firstRead = true;
  private Column[] outputColumns;
  private Column[] fullSchemaColumns;
  private ArrayRecord record;
  private int[] outputIndexes;
  // used for case where all input columns have been pruned (for case like COUNT(*))
  private boolean allColumnsPruned = false;
  final private ArrayRecord emptyRecord = new ArrayRecord(new Column[0]);
  final private ArrayList<String> emptyList =  new ArrayList<String>(0);

  public TextExtractor() {
    this.linebreakChar = '\n';
  }

  // no particular usage for execution context in this example
  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.inputs = inputs;
    this.attributes = attributes;
    // check if "delimiter" attribute is supplied via SQL query
    String columnDelimiter = this.attributes.getValueByKey("delimiter");
    if ( columnDelimiter != null) {
      if (columnDelimiter.length() == 1){
        this.delimiterChar = columnDelimiter.charAt(0);
      } else{
        throw new RuntimeException("column delimiter cannot be more than one character, sees: " + columnDelimiter);
      }
    } else {
      this.delimiterChar = ',';
    }
    System.out.println("TextExtractor set up with delimiter [" + this.delimiterChar + "].");
    // note: more properties can be inited from attributes if needed
    this.outputColumns = this.attributes.getRecordColumns();
    this.fullSchemaColumns = this.attributes.getFullTableColumns();
    this.record = new ArrayRecord(outputColumns);
    this.outputIndexes = this.attributes.getNeededIndexes();
    if (outputIndexes == null || outputIndexes.length == 0){
      allColumnsPruned = true;
    }
    if (!allColumnsPruned && (outputIndexes.length != outputColumns.length)){
      throw new IllegalArgumentException("Mismatched output schema: Expecting "
              + outputColumns.length + " columns but get " + outputIndexes.length);
    }
  }

  @Override
  public Record extract() throws IOException {
    List<String> parts;
    while(true){
      parts = readNextLine();
      if (parts == null) {
        return null;
      }
      if (this.allColumnsPruned) {
        return emptyRecord;
      }
      if (parts.isEmpty()){
        // empty line, ignore
        // TODO: a option to throw or ignore
        continue;
      } else {
        break;
      }
    }
    return textLineToRecord(parts);
  }

  @Override
  public void close(){
    // no-op
  }

  private Record textLineToRecord(List<String> parts) throws IllegalArgumentException,IOException
  {
    if (this.outputColumns.length != 0){
      int index = 0;
      for(int i = 0; i < parts.size(); i++){
        // only parse data in columns indexed by output indexes
        if (index < outputIndexes.length && i == outputIndexes[index]){
          String component = parts.get(i);
          // TODO: make NULL representation configurable
          if (component.equals("NULL")) {
            record.set(index, null);
            continue;
          }
          Object o = component;
          switch (outputColumns[index].getType()) {
            case STRING:
              break;
            case BIGINT:
              o = Long.parseLong(component);
              break;
            case BOOLEAN:
              o = Boolean.parseBoolean(component);
              break;
            case DOUBLE:
              o = Double.parseDouble(component);
              break;
            case FLOAT:
              o = Float.parseFloat(component);
              break;
            case BINARY:
              o = new Binary(component.getBytes());
              break;
            case DATETIME:
              o = Date.valueOf(component);
              break;
            case DECIMAL:
              o = new BigDecimal(component);
              break;
            case TINYINT:
            case INT:
            case SMALLINT:
              o = Integer.parseInt(component);
              break;
            // TODO: add support
            case CHAR:
            case VARCHAR:
            case ARRAY:
            case MAP:
            default:
              throw new IllegalArgumentException("Type " + outputColumns[index].getType() + " not supported for now.");
          }
          // TODO: change to setWithNoValidation when becomes available
          record.set(index, o);
          index++;
        }
      }
    }
    return record;
  }


  /**
   * parse a single text line read by the reader and return parts a list of string
   * revised from: https://github.com/agilepro/mendocino/blob/master/src/org/workcast/streams/CSVHelper.java
   * @param r reader to read a single line
   * @return text component separated by the specified delimiter
   * @throws IOException
   */
  public List<String> parseLine(Reader r) throws IOException{
    // optimized for count(*)
    int ch = r.read();
    if (this.allColumnsPruned){
      while (ch != this.linebreakChar){
        ch = r.read();
        if (ch < 0){
          return null;
        }
      }
      return emptyList;
    }
    boolean emptyLine = true;
    while (ch == '\r') {
      //ignore linefeed characters wherever they are, particularly just before end of file
      ch = r.read();
    }
    if (ch < 0) {
      return null;
    }
    ArrayList<String> parts = new ArrayList<String>();
    StringBuffer curPart = new StringBuffer();
    boolean hasQuotes = false;
    boolean quoteStarted = false;
    while (ch >= 0) {
      if (hasQuotes) {
        quoteStarted = true;
        if (ch == '\"') {
          hasQuotes = false;
        }
        else {
          curPart.append((char)ch);
        }
      }
      else {
        if (ch == '\"') {
          hasQuotes = true;
          if (quoteStarted) {
            // if this is the second quote in a value, add a quote to handle double quote in the middle of a value
            curPart.append('\"');
          }
        }
        else if (ch == this.delimiterChar && !quoteStarted) {
          parts.add(curPart.toString());
          curPart = new StringBuffer();
          quoteStarted = false;
        }
        else if (ch == '\r') {
          //ignore LF characters
        }
        else if (ch == this.linebreakChar) {
          if (emptyLine){
            return this.emptyList;
          } else {
            //end of a line, break out
            break;
          }
        }
        else {
          curPart.append((char)ch);
          emptyLine = false;
        }
      }
      ch = r.read();
    }
    parts.add(curPart.toString());
    if (parts.size() != this.fullSchemaColumns.length){
      String errorMsg = "SCHEMA MISMATCH: External Table schema specified a total of [" +
              this.fullSchemaColumns.length + "] columns, but current text line parsed into ["
              + parts.size() + "] columns delimited by [" + this.delimiterChar + "]. Current line is read as: "
              + StringUtils.join(parts.toArray(), this.delimiterChar);
      throw new RuntimeException(errorMsg);
    }
    return parts;
  }
  /**
   * Read next line from underlying input streams.
   * @return The next line as a collection of String objects, separated by line delimiter.
   * If all of the contents of input streams has been read, return null.
   */
  private List<String> readNextLine() throws IOException {
    if (firstRead) {
      firstRead = false;
      // the first read, initialize things
      currentReader = moveToNextStream();
      if (currentReader == null) {
        // empty input stream set
        return null;
      }
    }
    while (currentReader != null) {
      List<String> parts = parseLine(currentReader);
      if (parts != null) {
        return parts;
      }
      currentReader = moveToNextStream();
    }
    return null;
  }

  private BufferedReader moveToNextStream() throws IOException {
    SourceInputStream stream = inputs.next();
    if (stream == null) {
      return null;
    } else {
      System.out.println("Processing: " + stream.getFileName());
      return new BufferedReader(new InputStreamReader(stream));
    }
  }
}
