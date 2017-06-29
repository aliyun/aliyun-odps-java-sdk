package com.aliyun.odps.udf.example.text.withclone;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class TextExtractorWithClone extends Extractor {
  private InputStreamSet inputs;
  private int cloneCopy;
  private SourceInputStream currentInput;

  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.inputs = inputs;
    this.currentInput = null;
    this.cloneCopy = 10;
    try{
      while (true) {
        ArrayList<BufferedReader> cloneReaders = moveToNextFile();
        if (cloneReaders == null) {
          break;
        }
        int idx = 0;
        for (BufferedReader reader : cloneReaders) {
          // note that since we skipped the first i lines for ith cloned stream, this will effectively print
          // the first [cloneCopy] different lines of each file.
          System.out.println("reading a line from cloned stream # " + idx++);
          System.out.println(reader.readLine());
        }
      }
      long skippBytes = this.currentInput.skip(100);
      skippBytes += this.currentInput.skip(200);
      BufferedReader sourceReader = new BufferedReader(new InputStreamReader(currentInput));
      System.out.println("line content after skipping: " + skippBytes + " bytes.");
      System.out.println(sourceReader.readLine());
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  // this extractor is only for demo of the clone stream,
  // no meaning extract operations are implemented
  public Record extract() throws IOException {
    return null;
  }

  @Override
  public void close() {
    try{
      this.currentInput.close();
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  private ArrayList<BufferedReader> moveToNextFile() throws IOException {
    ArrayList<BufferedReader> bufferedReaders = new ArrayList<BufferedReader>();
    // each input will be cloned to a few copeis as defined by cloneCopy.
    SourceInputStream stream = inputs.next();
    if (stream == null) {
      return null;
    } else {
      if (this.currentInput != null) {
        this.currentInput.close();
      }
      this.currentInput = stream;
      System.out.println("--------------------------------------------------");
      System.out.println("Processing file " + this.currentInput.getFileName());
      for (int i = 0; i < cloneCopy; i++) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.currentInput.cloneStream()));
        // skip i lines for ith reader (i starts from 0)
        for (int j = 0; j < i; j++) {
          reader.readLine();
        }
        bufferedReaders.add(reader);
      }
      return bufferedReaders;
    }
  }
}
