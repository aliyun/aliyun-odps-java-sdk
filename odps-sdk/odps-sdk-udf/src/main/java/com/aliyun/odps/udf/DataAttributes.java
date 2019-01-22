package com.aliyun.odps.udf;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 *   Provides interfaces to access different attributes of the underlying data, including the attributes provided
 *   by the user, as well as different (system) properties that govern the underlying data, such as
 *   the record columns, resources used, etc.
 * </p>
 * <p>
 *   This interface is used only for MaxCompute framework to encapsulate the internal representation of attributes.
 *   It is not intended for user to implement it.
 * </p>
 */
public abstract class DataAttributes {

  /**
   * Get the attribute value associated with the given key, return null if key not found
   *
   * @return: attribute value
   **/
  public abstract String getValueByKey(String key);

  /**
   * Get a copy of all user-specified attributes. Note that the modification of the return value does not affect the
   * underlying storage of attributes.
   * @return All attributes specified by user
   */
  public abstract Map<String, String> getAttributes();

  /**
   * Get the customized external data location that describes external data storage location other than
   * storage types with built-in support (such as Aliyun OSS/TableStore), user is responsible for parsing and
   * connecting to customized data location in self-defined Extractor and/or Outputer
   * @return customized external location defined in LOCATION clause
   *
   * Note: when "oss://" or "tablestore://" scheme is used in LOCATION clause, this returns null instead
   */
  public abstract String getCustomizedDataLocation();

  /**
   * @param isExtractor flag to distinguish extractor or outputer
   * @return Serde properties specified in DDL statement and table information (e.g., columns name/type)
   */
  public abstract Properties getHiveTableProperties(boolean isExtractor);

  /**
   * Getter for columns describing expected Record schema: un-used columns may have been pruned and this schema
   * therefore can be either
   * 1. a subset of full schema that describes physical data underlying an EXTERNAL table, or
   * 2. a subset of joined schemas from multiple input tables
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
