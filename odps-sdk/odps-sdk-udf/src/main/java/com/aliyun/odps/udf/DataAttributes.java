package com.aliyun.odps.udf;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import java.util.HashSet;
import java.util.Properties;

/**
 * Provides interfaces to access different attributes of the underlying data, including the attributes provided
 * by the user, as well as different (system) properties that govern the underlying data, such as
 * the record columns, resources used, etc.
 */
public abstract class DataAttributes {

  /**
   * Get the attribute value associated with the given key, return null if key not found
   *
   * @return: attribute value
   **/
  public abstract String getValueByKey(String key);


  /**
   * @param isExtractor flag to distinguish extractor or outputer
   * @return Serde properties specified in DDL statement and table information (e.g., columns name/type)
   */
  public abstract Properties getHiveTableProperties(boolean isExtractor);

  /**
   * Getter for columns describing expected Record schema: un-used columns may have been pruned and this schema
   * therefore can be a subset of full schema (that describes underlying physical data)
   *
   * @return: column arrays
   */
  public abstract Column[] getRecordColumns();

  /**
   * Getter for record columns describing the FULL schema of underlying physical data, represented by the
   * (external) table
   *
   * @return: column arrays
   */
  public abstract Column[] getFullTableColumns();

  /**
   * Getter for needed indexes, this can be used to skip deserialization of non-needed column(s).
   *
   * @return: the indexes of columns that should be extracted from underlying unstructured raw data.
   **/
  public abstract int[] getNeededIndexes();

  /**
   * Getter for set of resources.
   *
   * @return: resources, each denoted by a formatted string of <project>:<resourceName>
   **/
  public abstract HashSet<String> getResources();

  /**
   * Check if the Column schemas passed in by data attributes matched the expectation. A runtime exception
   * will be thrown if there is a mismatch for the schema
   *
   **/
  public abstract void verifySchema(OdpsType[] expectedSchemas);
}
