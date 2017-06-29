package com.aliyun.odps.udf.example.image;

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

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class ImageExtractor extends Extractor{
  private InputStreamSet inputs;
  private DataAttributes attributes;
  private ImageReader reader;
  private int targetedImageWidth = 1024;
  private Column[] outputColumns;
  private ArrayRecord record;
  private String inputImageFormat;
  private String transformedImageFormat;
  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.inputs = inputs;
    this.attributes = attributes;
    this.outputColumns  = this.attributes.getRecordColumns();
    this.record = new ArrayRecord(outputColumns);

    this.attributes.verifySchema(new OdpsType[]{ OdpsType.STRING, OdpsType.BIGINT, OdpsType.BIGINT, OdpsType.BINARY });
    this.inputImageFormat = this.attributes.getValueByKey("inputImageFormat");
    this.transformedImageFormat = this.attributes.getValueByKey("transformedImageFormat");
    if (this.inputImageFormat == null || this.transformedImageFormat == null){
      throw new RuntimeException("Both input image format and output image format must be specified.");
    }
    Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName(this.inputImageFormat);
    // get the first reader that can read specified input image format
    this.reader = readers.next();
  }

  @Override
  // read each image, if image's width is smaller than targetWidth, rescale it to targetWidth (assuming square images)
  // and store the image name, resulted image width, resulted image heigth raw bytes into record (skipped images not stored)
  // input image format and the transformed image format are specified by  parameters specified in attributes (via SERDEPROPERTIES)
  public Record extract() throws IOException {
    while(true){
      SourceInputStream input = inputs.next();
      if (input == null){
        return null;
      }
      String fileName = input.getFileName();
      fileName = fileName.substring(fileName.lastIndexOf('/') + 1);
      ImageDimension dimension = getImageDimension(input);
      // skipping images with width already larger than targeted width
      // this is just an imaginary requirement for demo purpose
      if (dimension.getWidth()> this.targetedImageWidth){
        System.out.println("Skipping processing image " + input.getFileName()
                + " since its width is already larger than " + this.targetedImageWidth);
        continue;
      } else {
        BufferedImage image = reader.read(0);
        ByteArrayOutputStream buffer = getTransformedImage(image);

        this.record.setString(0, fileName.substring(0, fileName.lastIndexOf('.')));
        this.record.setBigint(1, (long)this.targetedImageWidth);
        this.record.setBigint(2, (long)this.targetedImageWidth);
        this.record.setBinary(3, new Binary(buffer.toByteArray()));
        return this.record;
      }
    }
  }

  @Override
  public void close() {
    // no-op
  }

  // get image dimension (by reading only meta info, instead of the whole image)
  private ImageDimension getImageDimension(InputStream input) throws IOException {
    ImageInputStream imageInput = ImageIO.createImageInputStream(input);
    reader.setInput(imageInput, true);
    return new ImageDimension(reader.getWidth(0), reader.getHeight(0));
  }

  // scale input image to targetedImageWidth, transform it to the format of transformedImageFormat and return the bytes
  private ByteArrayOutputStream getTransformedImage(BufferedImage inputImage) throws IOException{
    BufferedImage scaledImage = new BufferedImage(this.targetedImageWidth, this.targetedImageWidth, BufferedImage.TYPE_BYTE_GRAY);
    Graphics g = scaledImage.createGraphics();
    g.drawImage(inputImage, 0, 0, this.targetedImageWidth, this.targetedImageWidth, null);
    g.dispose();
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    ImageIO.write(scaledImage, this.transformedImageFormat, buffer);
    return buffer;
  }

  private class ImageDimension
  {
    private long width;
    private long height;
    ImageDimension(long width, long height){
      this.width = width;
      this.height = height;
    }
    long getWidth(){
      return this.width;
    }
    long getHeight(){
      return this.height;
    }
  }
}

