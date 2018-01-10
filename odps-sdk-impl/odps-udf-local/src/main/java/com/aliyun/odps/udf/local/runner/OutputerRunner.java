package com.aliyun.odps.udf.local.runner;

import com.aliyun.odps.Column;
import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.Outputer;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.LocalDataAttributes;
import com.aliyun.odps.udf.local.util.fs.LocalOutputStreamSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutputerRunner extends ExtendedRunner{
  private Outputer outputer;
  private LocalDataAttributes attributes;
  private boolean outputerSetup;
  private Column[] tableSchema;
  private List<Record> records;
  private boolean outputToLocalFs;

  public OutputerRunner(Odps odps, Outputer outputer, DataAttributes localAttributes) {
    super(odps);
    if (outputer == null){
      throw new IllegalArgumentException("Missing arguments:outputer");
    }
    this.outputer = outputer;
    if (!(localAttributes instanceof LocalDataAttributes)) {
      throw new UnsupportedOperationException("only accepts LocalDataAttributes.");
    }
    this.attributes = (LocalDataAttributes)localAttributes;
    this.outputerSetup = false;
    this.tableSchema = localAttributes.getFullTableColumns();
    this.records = new ArrayList<Record>();
    this.outputToLocalFs = true;
  }

  @Override
  protected BaseRunner internalFeed(Object[] input) throws LocalRunException {
    this.records.add(new ArrayRecord(this.tableSchema, input));
    return this;
  }

  @Override
  public void feedRecords(List<Record> records) throws LocalRunException {
    for (Record record : records){
      this.records.add(record.clone());
    }
  }

  @Override
  public void yieldTo(String location) throws LocalRunException {
    try {
      LocalOutputStreamSet outputStreamSet = null;
      if (this.outputToLocalFs){
        outputStreamSet = new LocalOutputStreamSet(location);
      }
      if (!this.outputerSetup) {
        this.attributes.setCustomizedDataLocation(location);
        this.outputer.setup(
            this.context,
            outputStreamSet,
            this.attributes);
        this.outputerSetup = true;
      }
      for (Record record : records){
        this.outputer.output(record);
      }
      this.outputer.close();
      if(this.outputToLocalFs) {
        outputStreamSet.close();
      }
    } catch (IOException e) {
      throw new LocalRunException(e.toString());
    }
  }

  @Override
  protected List<Object[]> internalYield() throws LocalRunException {
    throw new NotImplementedException("internalYield() not supported for OutputerRunner.");
  }

  @Override
  public List<Object[]> yield() throws LocalRunException {
    throw new NotImplementedException("internalYield() not supported for OutputerRunner.");
  }

  public void setUseCustomizedOutput(){
    this.outputToLocalFs = false;
  }
}
