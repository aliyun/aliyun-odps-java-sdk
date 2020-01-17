package com.aliyun.odps.udf.local.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.udf.DataAttributes;

import java.util.*;

public class LocalDataAttributes extends DataAttributes {
  private Map<String, String> attributes;
  private Column[] fullTableSchema;
  private Column[] outputSchema;
  private int[] neededIndexes;
  private static final String CUSTOMIZED_LOCATION = "__odps.local.internal.customized.location";

  public LocalDataAttributes(
      Map<String, String> serdeProperties,
      Column[] externalTableSchema) {
    this(serdeProperties, externalTableSchema, null);
  }

  public LocalDataAttributes(
      Map<String, String> serdeProperties,
      Column[] externalTableSchema,
      int[] neededIndexes) {
    // initiate attributes to be user-specified serde properties.
    this.attributes = serdeProperties == null ? new HashMap<String, String>() : serdeProperties;

    this.fullTableSchema = externalTableSchema;

    if (neededIndexes == null) {
      this.neededIndexes = new int[externalTableSchema.length];
      for (int i = 0; i < this.neededIndexes.length; i++){
        this.neededIndexes[i] = i;
      }
      this.outputSchema = externalTableSchema;
    } else {
      if (neededIndexes.length > 0){
        if (neededIndexes.length > externalTableSchema.length
            || neededIndexes[neededIndexes.length - 1] > externalTableSchema.length) {
          throw new UnsupportedOperationException("Invalid needed indexes: " + Arrays.toString(neededIndexes));
        }
      }
      this.neededIndexes = neededIndexes;
      this.outputSchema = new Column[this.neededIndexes.length];
      for (int i = 0 ; i < neededIndexes.length; i++) {
        this.outputSchema[i] = this.fullTableSchema[neededIndexes[i]];
      }
    }
  }

  @Override
  public String getValueByKey(String key) {
    return this.attributes.get(key);
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public String getCustomizedDataLocation() {
    return this.attributes.get(CUSTOMIZED_LOCATION);
  }

  public void setCustomizedDataLocation(String location) {
    this.attributes.put(CUSTOMIZED_LOCATION, location);
  }

  @Override
  public Properties getHiveTableProperties() {
    throw new UnsupportedOperationException("getHiveTableProperties not supported for local mode");
  }

  @Override
  public Column[] getRecordColumns() {
    return this.outputSchema;
  }

  @Override
  public Column[] getFullTableColumns() {
    return this.fullTableSchema;
  }

  @Override
  public int[] getNeededIndexes() {
    return this.neededIndexes;
  }

  @Override
  public HashSet<String> getResources() {
    throw new UnsupportedOperationException("getResources not supported for local mode");
  }

  @Override
  public void verifySchema(OdpsType[] expectedSchemas) {
    if (this.outputSchema.length != expectedSchemas.length ){
      throw new RuntimeException("Expecting to handle schema of " +
          expectedSchemas.length + " columns, but sees " + this.outputSchema.length);
    }
    for (int i = 0 ; i < outputSchema.length; i++){
      if (!outputSchema[i].getTypeInfo().getOdpsType().equals(expectedSchemas[i])){
        throw new RuntimeException("Schema for column [" + i + "] mismatches: expecting " + expectedSchemas[i]
            + " but sees " +  outputSchema[i].getTypeInfo().getOdpsType());
      }
    }
  }
}
