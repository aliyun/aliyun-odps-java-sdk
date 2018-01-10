package com.aliyun.odps.udf.local.runner;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.LocalDataAttributes;
import com.aliyun.odps.udf.local.util.fs.LocalInputStreamSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ExtractorRunner extends ExtendedRunner{
  private Extractor extractor;

  private boolean extractorSetup;
  private LocalDataAttributes attributes;
  public ExtractorRunner(Odps odps, Extractor extractor, DataAttributes localAttributes) {
    super(odps);
    if (extractor == null){
      throw new IllegalArgumentException("Missing arguments:extractor");
    }
    this.extractor = extractor;
    if (!(localAttributes instanceof LocalDataAttributes)) {
      throw new UnsupportedOperationException("only accepts LocalDataAttributes.");
    }
    this.attributes = (LocalDataAttributes)localAttributes;
    this.files = new ArrayList<File>();
    this.extractorSetup = false;
  }

  @Override
  public List<Record> yieldRecords() throws LocalRunException {
    if (!this.extractorSetup) {
      this.extractor.setup(
          this.context,
          new LocalInputStreamSet(this.files),
          this.attributes);
      this.extractorSetup = true;
    }
    List<Record> records = new ArrayList<Record>();
    try {
      while (true) {
        Record record = this.extractor.extract();
        if (record == null) {
          break;
        } else {
          records.add(record.clone());
        }
      }
    } catch (IOException e) {
      throw new LocalRunException(e.toString());
    }
    this.extractor.close();
    this.extractor = null;
    return records;
  }

  @Override
  public void feedFiles(List<String> paths) throws LocalRunException {
    // TODO: support feeding format of <filePath>:<start>:<end>
    for (String path : paths) {
      File inputLocation = new File(path);
      if (!inputLocation.exists()) {
        throw new UnsupportedOperationException(inputLocation.getAbsolutePath() + " does not exist.");
      }
      if (inputLocation.isFile()) {
        this.files.add(inputLocation);
      } else if (inputLocation.isDirectory()) {
        // recursive list all files inside the directory
        // note: validness of localDirectoryPath should have been checked in LocalDataAttributes ctor
        Collection<File> filesCollection = FileUtils.listFiles(
            inputLocation, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        File[] directoryFiles = filesCollection.toArray(new File[filesCollection.size()]);
        Arrays.sort(directoryFiles);
        this.files.addAll(Arrays.asList(directoryFiles));
      } else {
        throw new UnsupportedOperationException(inputLocation + "refers to unknown file type - this should not happen");
      }
    }
  }

  @Override
  public void feedDirectory(String directory) throws LocalRunException {
    // feedFiles() handles both file and directory
    feedFiles(Arrays.asList(directory));
  }

  @Override
  protected List<Object[]> internalYield() throws LocalRunException {
    List<Record> records = yieldRecords();
    for (Record record : records){
     this.buffer.add(record.toArray());
    }
    return buffer;
  }

  @Override
  protected BaseRunner internalFeed(Object[] input) throws LocalRunException {
    List<String> paths = new ArrayList<String>(input.length);
    for (int i = 0; i < input.length; i++) {
      if (!(input[i] instanceof String)) {
        throw new UnsupportedOperationException("must feed file path as String to ExtractorRunner");
      }
      paths.add((String) input[i]);
    }
    feedFiles(paths);
    return this;
  }

  @Override
  public BaseRunner feedAll(Object[][] inputs) throws LocalRunException{
    throw new UnsupportedOperationException("feedAll not supported for ExtractorRunner all input" +
        " must be added through feedDirectory(), feedFiles(), or feed()");
  }

  @Override
  public BaseRunner feedAll(List<Object[]> inputs) throws LocalRunException {
    throw new UnsupportedOperationException("feedAll not supported for ExtractorRunner all input" +
        " must be added through feedDirectory(), feedFiles(), or feed()");
  }
}
