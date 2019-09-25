package com.aliyun.odps.udf.example.text;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

/**
 * Text extractor that extract schematized records from formatted plain-text(csv, tsv etc.)
 **/
public class TextExtractor extends Extractor {

  private InputStreamSet inputs;
  private char delimiterChar;
  private char linebreakChar;
  private DataAttributes attributes;
  private Reader currentReader;
  private boolean firstRead = true;
  private Column[] outputColumns;
  private Column[] fullSchemaColumns;
  private String[] lineParts;
  private OdpsType[] outputTypes;
  private ArrayRecord record;
  private int[] outputIndexes;
  private boolean complexText;
  private boolean isGzip;
  private boolean strict = true;
  private boolean ignoreLineFeed = true;
  private boolean handleQuote = true;
  // used for case where all input columns have been pruned (for case like COUNT(*))
  private boolean allColumnsPruned = false;
  final private ArrayRecord emptyRecord = new ArrayRecord(new Column[0]);
  final private ArrayList<String> emptyList =  new ArrayList<String>(0);
  private ExecutionContext ctx;

  public TextExtractor() {
    this.linebreakChar = '\n';
    this.complexText = false;
    this.isGzip = false;
  }

  // no particular usage for execution context in this example
  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.inputs = inputs;
    this.attributes = attributes;
    this.ctx = ctx;
    // check if "delimiter" attribute is supplied via SQL query
    String columnDelimiter = this.attributes.getValueByKey("delimiter");
    if ( columnDelimiter != null) {
      if (columnDelimiter.length() == 1){
        this.delimiterChar = columnDelimiter.charAt(0);
      } else{
        throw new IllegalArgumentException("column delimiter cannot be more than one character, sees: " + columnDelimiter);
      }
    } else {
      this.delimiterChar = ',';
    }
    String lineTerminator = attributes.getValueByKey("line.terminator");
    if (lineTerminator != null && !lineTerminator.isEmpty()) {
      if (lineTerminator.length() > 1) {
        throw new IllegalArgumentException("line terminator cannot be more than one character, sees: " + lineTerminator);
      }
      linebreakChar = lineTerminator.charAt(0);
    }
    String isComplexText = this.attributes.getValueByKey("odps.text.option.complex.text.enabled");
    if ( isComplexText != null && isComplexText.toLowerCase().equals("true")) {
      this.complexText = true;
    }

    String gzip = this.attributes.getValueByKey("odps.text.option.gzip.input.enabled");
    if ( gzip != null && gzip.toLowerCase().equals("true")) {
      this.isGzip = true;
    }

    String strictMode = attributes.getValueByKey("odps.text.option.strict.mode");
    if (!StringUtils.isNullOrEmpty(strictMode)) {
      strict = Boolean.valueOf(strictMode);
    }

    String ignoreLineFeedStr = attributes.getValueByKey("odps.text.option.ignore.line.feed");
    if (!StringUtils.isNullOrEmpty(ignoreLineFeedStr)) {
      ignoreLineFeed = Boolean.valueOf(ignoreLineFeedStr);
    }

    String quoteEnable = attributes.getValueByKey("odps.text.option.quote.enable");
    if (!StringUtils.isNullOrEmpty(quoteEnable)) {
      handleQuote = Boolean.valueOf(quoteEnable);
    }

    System.out.println(
      org.apache.commons.lang.StringEscapeUtils.escapeJava(("TextExtractor set up with delimiter [" + this.delimiterChar + "], " +
        " line terminator [" + linebreakChar + "], with complex text flag set to "
        + this.complexText + " and reading gzip file set to " + this.isGzip)));
    // note: more properties can be inited from attributes if needed
    this.outputColumns = this.attributes.getRecordColumns();
    this.outputTypes = new OdpsType[this.outputColumns.length];
    for (int i = 0; i < this.outputTypes.length; i++){
      this.outputTypes[i] = this.outputColumns[i].getType();
    }
    this.fullSchemaColumns = this.attributes.getFullTableColumns();
    this.lineParts = new String[this.fullSchemaColumns.length];
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
    String[] parts;
    while(true){
      parts = readNextLine();
      if (parts == null) {
        return null;
      }
      if (this.allColumnsPruned) {
        return emptyRecord;
      }
      if (parts.length == 0){
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

  private Record textLineToRecord(String[] parts) throws IllegalArgumentException,IOException
  {
    if (this.outputColumns.length != 0){
      int index = 0;
      for(int i = 0; i < parts.length; i++){
        // only parse data in columns indexed by output indexes
        if (index < outputIndexes.length && i == outputIndexes[index]){
          // TODO: make NULL representation configurable
          if (parts[i].equals("NULL")) {
            record.set(index, null);
            index++;
            continue;
          }
          switch (this.outputTypes[index]) {
            case STRING:
              record.set(index,parts[i]);
              break;
            case BIGINT:
              record.setBigint(index,Long.parseLong(parts[i]));
              break;
            case BOOLEAN:
              record.setBoolean(index, Boolean.parseBoolean(parts[i]));
              break;
            case DOUBLE:
              record.setDouble(index, Double.parseDouble(parts[i]));
              break;
            case FLOAT:
              record.setFloat(index, Float.parseFloat(parts[i]));
              break;
            case BINARY:
              record.setBinary(index, new Binary(parts[i].getBytes()));
              break;
            case DATETIME:
              record.setDate(index, Date.valueOf(parts[i]));
              break;
            case DECIMAL:
              record.setDecimal(index, new BigDecimal(parts[i]));
              break;
            case TINYINT:
            case INT:
            case SMALLINT:
              record.setInt(index, Integer.parseInt(parts[i]));
              break;
            // TODO: add support
            case CHAR:
            case VARCHAR:
            case ARRAY:
            case MAP:
            default:
              throw new IllegalArgumentException("Type " + this.outputTypes[index] + " not supported for now.");
          }
          // TODO: change to setWithNoValidation when becomes available
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
  public String[] parseLine(Reader r) throws IOException{
    // optimized for count(*)
    int ch = r.read();
    if (this.allColumnsPruned){
      while (ch != this.linebreakChar){
        ch = r.read();
        if (ch < 0){
          return null;
        }
      }
      return new String[0];
    }
    boolean emptyLine = true;
    int colIndx = 0;
    if (ignoreLineFeed) {
      while (ch == '\r') {
        //ignore linefeed characters wherever they are, particularly just before end of file
        ch = r.read();
      }
    }
    if (ch < 0) {
      return null;
    }
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
        if (ch == '\"' && handleQuote) {
          hasQuotes = true;
          if (quoteStarted) {
            // if this is the second quote in a value, add a quote to handle double quote in the middle of a value
            curPart.append('\"');
          }
        }
        else if (ch == this.delimiterChar && !quoteStarted) {
          setLinePart(colIndx++, curPart.toString());
          curPart = new StringBuffer();
          quoteStarted = false;
        }
        else if (ch == '\r' && ignoreLineFeed) {
          //ignore LF characters
        }
        else if (ch == this.linebreakChar) {
          if (emptyLine){
            return new String[0];
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
    setLinePart(colIndx++, curPart.toString());
    if (colIndx != fullSchemaColumns.length) {
      handleMismatchLine(colIndx);
    }
    return lineParts;
  }

  private void setLinePart(int idx, String value) {
    if (idx >= fullSchemaColumns.length) {
      handleMismatchLine(idx);
    } else {
      lineParts[idx] = value;
    }
  }

  private void handleMismatchLine(int colIndx) {
    if (colIndx < fullSchemaColumns.length) {
      ctx.getCounter("text.parse", "schema.partial").increment(1);
      // fill remaining columns with NULLs
      for (int i = colIndx; i < fullSchemaColumns.length; i++) {
        lineParts[i] = "NULL";
      }
    } else {
      ctx.getCounter("text.parse", "schema.oversize").increment(1);
    }
    String errorMsg = "SCHEMA MISMATCH: External Table schema specified a total of [" +
      this.fullSchemaColumns.length + "] columns, but current text line parsed into ["
      + colIndx + "] columns delimited by [" + this.delimiterChar + "]. Current line is read as: "
      + StringUtils.join(this.lineParts, this.delimiterChar);
    errorMsg = StringEscapeUtils.escapeJava(errorMsg);
    if (strict) {
      throw new RuntimeException(errorMsg);
    }
    System.err.println(errorMsg);
  }

  /**
   * Read next line from underlying input streams.
   * @return The next line as a collection of String objects, separated by line delimiter.
   * If all of the contents of input streams has been read, return null.
   */
  private String[] readNextLine() throws IOException {
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
      if (this.complexText){
      String[] parts = parseLine(currentReader);
      if (parts != null) {
          return parts;
        }
      } else {
        String line = ((BufferedReader)currentReader).readLine();
        if (line != null) {
          return StringUtils.splitPreserveAllTokens(line, this.delimiterChar);
        }
      }
      currentReader = moveToNextStream();
    }
    return null;
  }

  private Reader moveToNextStream() throws IOException {
    SourceInputStream stream = inputs.next();
    if (stream == null) {
      return null;
    } else {
      long splitSize = stream.getSplitSize();
      if (stream.getFileSize() != splitSize) {
        this.complexText = true;
        long currentPos = stream.getCurrentPos();
        long splitStart  = stream.getSplitStart();
        if (currentPos < splitStart){
          System.out.println("Skipping: " + (splitStart - currentPos) + " bytes to split start.");
          stream.skip(splitStart - currentPos);
          currentPos = stream.getCurrentPos();
        }
        System.out.println("Processing bytes [" + currentPos + " , " +  (currentPos + splitSize - 1) + "] for file "+ stream.getFileName());
        return new SplitReader(new BufferedReader(new InputStreamReader(stream)), splitSize);
      } else {
        System.out.println("Processing whole file: " + stream.getFileName());
        Reader reader;
        if (this.isGzip){
          reader = new InputStreamReader(new GZIPInputStream(stream));
        } else {
          reader = new InputStreamReader(stream);
        }
        return new BufferedReader(reader);
      }
    }
  }

}
