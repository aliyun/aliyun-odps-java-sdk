package com.aliyun.odps.udf;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.serde.SerDeException;
import com.aliyun.odps.io.Writable;

import java.util.Properties;

/**
 * SerDe interface for ODPS record to and from {@link com.aliyun.odps.io.Writable}
 */
public abstract class RecordSerDe {

  /**
   * Initialize the SerDe.
   *
   * @param configuration        Odps configuration
   * @param tableProperties      Table properties
   * @throws SerDeException
   */
  public abstract void initialize(Configuration configuration, Properties tableProperties) throws SerDeException;

  /**
   * Serialize a ODPS record into Writable {@link com.aliyun.odps.io.Writable}. In most cases, the return value
   * of this function will be constant since the function will reuse the Writable object. If the client
   * wants to keep a copy of the Writable, the client needs to clone the returned value.
   */
  public abstract Writable serialize(Record record) throws SerDeException;

  /**
   * Deserialize an Record out of a Writable blob.
   *
   * @param blob
   *          The Writable object containing a serialized object
   * @return A Record object representing the contents in the blob.
   */
  public abstract Record deserialize(Writable blob) throws SerDeException;

  // HiveOutputFormat need serialized class to init
  public abstract Class getSerializedClass();
}
