package com.aliyun.odps.udf.example.image;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.io.SinkOutputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Outputer;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ImageOutputer extends Outputer {
  private OutputStreamSet outputStreamSet;
  private DataAttributes attributes;
  private String outputFormat = "jpg";
  private float sobel[] = {
          1.0f, 0.0f, -1.0f,
          2.0f, 0.0f, -2.0f,
          1.0f, 0.0f, -1.0f
  };

  @Override
  public void setup(ExecutionContext ctx, OutputStreamSet outputStreamSet, DataAttributes attributes) {
    this.outputStreamSet = outputStreamSet;
    this.attributes = attributes;
    this.attributes.verifySchema(new OdpsType[]{ OdpsType.STRING, OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.BINARY });
  }

  @Override
  public void output(Record record) throws IOException {
    String name = record.getString(0);
    Long width = record.getBigint(1);
    Long height = record.getBigint(2);
    ByteArrayInputStream input =  new ByteArrayInputStream(record.getBytes(3));
    BufferedImage sobelEdgeImage = getEdgeImage(input);
    OutputStream outputStream = this.outputStreamSet.next(name + "_" + width + "x" + height + "." + outputFormat);
    ImageIO.write(sobelEdgeImage, this.outputFormat, outputStream);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  // apply sobel operator to images for edge detection
  private BufferedImage getEdgeImage(InputStream input) throws IOException{
    BufferedImage image = ImageIO.read(input);
    BufferedImageOp sobelFilter = new ConvolveOp(new Kernel(3, 3, sobel));
    return sobelFilter.filter(image, null);
  }
}
