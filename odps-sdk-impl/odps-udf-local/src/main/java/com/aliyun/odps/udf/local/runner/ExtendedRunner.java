package com.aliyun.odps.udf.local.runner;


import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.InputSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.util.Collection;
import java.util.List;

public abstract class ExtendedRunner extends BaseRunner{
  protected List<File> files;
  protected List<Record> records;
  public ExtendedRunner(Odps odps) {
    super(odps);
  }

  protected List<Record> yieldRecords() throws LocalRunException {
    throw new NotImplementedException("No default implementation for yieldRecords.");
  }

  protected void yieldTo(String location) throws LocalRunException {
    throw new NotImplementedException("No default implementation for yieldTo.");
  }

  protected void feedFiles(List<String> files) throws LocalRunException {
    throw new NotImplementedException("No default implementation for feedFiles.");
  }

  protected void feedDirectory(String directory) throws LocalRunException {
    throw new NotImplementedException("No default implementation for feedDirectory.");
  }

  protected void feedRecords(List<Record> records) throws LocalRunException {
    throw new NotImplementedException("No default implementation for feedRecords.");
  }

  @Override
  public void addInputSource(InputSource inputSource){
    throw new UnsupportedOperationException("addInputSource not supported for ExtendedRunner.");
  }
}
