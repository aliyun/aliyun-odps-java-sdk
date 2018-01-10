package com.aliyun.odps.udf.example.weather;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;

import com.aliyun.odps.udf.UDFException;
import ucar.nc2.grib.grib1.*;
import ucar.nc2.grib.grib1.tables.Grib1Customizer;
import ucar.nc2.grib.grib1.tables.Grib1ParamTables;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Formatter;

public class NetCdfExtractor extends Extractor {

  private InputStreamSet inputs;
  private DataAttributes attributes;
  private Column[] outputColumns;
  private File localInputDirectory;
  private ExecutionContext ctx;
  private File currentInputFile;
  private Grib1RecordScanner currentGrib1Reader;
  private RandomAccessFile currentRaf;
  private Date reportDate;
  private int year;
  private long outputRecordCount;

  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.ctx = ctx;
    this.inputs = inputs;
    this.attributes = attributes;
    this.currentInputFile = null;
    this.currentGrib1Reader = null;
    this.currentRaf = null;
    this.year = 2015; // default
    this.outputRecordCount = 0;

    this.outputColumns = this.attributes.getRecordColumns();
    if (this.outputColumns.length != 14) {
      throw new RuntimeException("Expecting to extract 14 output columns " +
              "But sees " + outputColumns.length + " output columns provided in schema.");
    }
    // get year value via serde properties
    String yearStr = this.attributes.getValueByKey("year");
    if ( yearStr != null)
    {
      this.year = Integer.parseInt(yearStr);
    }

    // TODO: do type checking for all columns
    this.localInputDirectory = new File(System.getProperty("user.dir"), "tmpinput");
    if (!this.localInputDirectory.exists()) {
      this.localInputDirectory.mkdirs();
    }
  }

  @Override
  public Record extract() throws IOException {
    if (this.currentInputFile == null) {
      // download the first file (if any)
      if (!TryMovingNext()) {
        return null;
      }
    }
    Record record = GetRecordFromGrib1File();
    if (record == null) {
      if (!TryMovingNext()){
        return null;
      }
      else{
        // try again, note EMPTY grib1 file may cause premature termination, but that should not happen
        return GetRecordFromGrib1File();
      }
    } else {
      return record;
    }
  }


  private boolean TryMovingNext() throws  IOException {
    while(true){
      SourceInputStream inputStream = this.inputs.next();
      if (inputStream == null) {
        // done processing all files, no more file to be processed
        return false;
      }
      String fileName = getFileName(inputStream.getFileName());
      String tokens[] = fileName.split("_", -1);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
      try {
        this.reportDate = sdf.parse(tokens[4]);
      } catch (ParseException e) {
        throw new RuntimeException("Cannot parse data from file name " + fileName);
      }
      if (sdf.getCalendar().get(Calendar.YEAR) != this.year) {
        System.err.println("Skipping file [" + fileName + "] since it is not data for " + this.year);
        // continue
      } else {
        this.currentInputFile = downloadGribFielToLocal(inputStream);
        return true;
      }
    }
  }

  @Override
  public void close() {
    System.err.println(new Date() + ": Total " + this.outputRecordCount + " records extracted.");
    // clean things up
    clearDirectory(this.localInputDirectory);
    this.localInputDirectory.delete();
  }

  private void clearDirectory(File directory) {
    if (directory.isDirectory()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
    }
  }

  private String getFileName(String fullFilePath) {
    return fullFilePath.substring(fullFilePath.lastIndexOf('/') + 1);
  }

  private File downloadGribFielToLocal(SourceInputStream input) {
    File localFile;
    final int bufferSize = 10 * 1024 * 1024;
    byte[] bytes = new byte[bufferSize];
    try {
      String fileName = this.getFileName(input.getFileName());
      localFile = new File(this.localInputDirectory, fileName);
      if (localFile.exists()) {
        localFile.delete();
      }
      localFile.createNewFile();
      OutputStream outputStream = new FileOutputStream(localFile);
      long readTimes = 0;
      System.err.println(new Date() + ": Begin downloading " + fileName);
      while (true) {
        int readBytes = input.read(bytes);
        // Downloading could be time consuming, we need to claim alive to avoid timeout.
        if (readTimes++ % 10 == 0) {
          this.ctx.claimAlive();
        }
        try {
          outputStream.write(bytes, 0, readBytes);
        } catch (IOException e) {
          input.close();
          outputStream.close();
          throw e;
        }
        if (readBytes < bufferSize) {
          break;
        }
      }
      input.close();
      outputStream.close();
      System.err.println(new Date() + ": Done downloading " + fileName +
              ". Total size:[" + (input.getFileSize() >> 20) + "]MB.");
    } catch (IOException e) {
      throw new RuntimeException("Failed to write input to local directory", e);
    }
    return localFile;
  }

  // There seems to be no proper way of reading grib file directly as in-memory stream, so we download grib file locally
  // first before parsing the file
  private Record GetRecordFromGrib1File() {
    try {
      if (this.currentInputFile == null) {
        return null;
      }
      if (this.currentGrib1Reader == null) {
        this.currentRaf = new RandomAccessFile(this.currentInputFile.getPath(), "r");
        this.currentGrib1Reader = new Grib1RecordScanner(this.currentRaf);
        if (!this.currentGrib1Reader.isValidFile(this.currentRaf)) {
          throw new RuntimeException("No a valid grib1 file");
        }
      }

      if (this.currentGrib1Reader.hasNext()) {
        ucar.nc2.grib.grib1.Grib1Record gr1 = this.currentGrib1Reader.next();

        Grib1SectionGridDefinition gd = gr1.getGDSsection();
        Grib1Gds gds = gd.getGDS();
        Grib1Gds.LatLon latLon = (Grib1Gds.LatLon) gds;

        Grib1SectionProductDefinition pds = gr1.getPDSsection();

        Grib1Customizer cust = Grib1Customizer.factory(gr1, new Grib1ParamTables());
        Grib1Parameter parameter = cust.getParameter(pds.getCenter(),
                pds.getSubCenter(),
                pds.getTableVersion(),
                pds.getParameterNumber());

        //pds.showPds(cust, new Formatter(System.out));

        Grib1ParamLevel level = cust.getParamLevel(pds);
        Grib1SectionBinaryData bd = gr1.getDataSection();

        this.currentRaf.seek(bd.getStartingPosition());

        Grib1DataReader dataReader = new Grib1DataReader(pds.getDecimalScale(),
                gds.getScanMode(),
                gds.getNxRaw(),
                gds.getNyRaw(),
                gds.getNpts(),
                bd.getStartingPosition());

        byte[] bitmap = null;
        if (pds.bmsExists()) {
          Grib1SectionBitMap bitmapSec = gr1.getBitMapSection();
          bitmap = bitmapSec.getBitmap(this.currentRaf);
        }

        float[] values = dataReader.getData(this.currentRaf, bitmap);

        StringBuffer sb = new StringBuffer();
        if (values.length > 0) {
          sb.append(values[0]);
          for (int j = 1; j < values.length; j++) {
            sb.append(",");
            sb.append(values[j]);
          }
        }

        Record record = new ArrayRecord(this.outputColumns);
        record.setBigint(0, (long) gds.getNx());
        record.setBigint(1, (long) gds.getNy());
        record.setDouble(2, (double) latLon.la1);
        record.setDouble(3, (double) latLon.la2);
        record.setDouble(4, (double) latLon.lo1);
        record.setDouble(5, (double) latLon.lo2);
        record.setDouble(6, (double) latLon.deltaLat);
        record.setDouble(7, (double) latLon.deltaLon);
        record.setString(8, parameter.getName());
        record.setString(9, reportDate.toString());
        record.setString(10, pds.getReferenceDate().toDate().toString());
        record.setBigint(11, (long) (level.getLevelType()));
        record.setBigint(12, (long) (pds.getNmissing()));
        record.setString(13, sb.toString());
        this.outputRecordCount++;
        return record;
      } else {
        // close and set null to trigger processing of next file (if any)
        this.currentRaf.close();
        this.currentRaf = null;
        this.currentInputFile = null;
        this.currentGrib1Reader = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
