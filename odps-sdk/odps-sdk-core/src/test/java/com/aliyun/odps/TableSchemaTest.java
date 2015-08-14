package com.aliyun.odps;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

/**
 * Created by nizheming on 15/6/8.
 */
public class TableSchemaTest {

  @Test
  public void testSetColumns() throws Exception {
    TableSchema schema = new TableSchema();
    ArrayList<Column> columns = new ArrayList<Column>();
    columns.add(new Column("c0", OdpsType.STRING, "c0"));
    columns.add(new Column("c1", OdpsType.BIGINT));
    schema.setColumns(columns);
    assertEquals(schema.getColumns().size(), 2L);
    assertEquals(schema.getColumn("c0").getName(), "c0");
    assertEquals(schema.getColumn("c0").getType(), OdpsType.STRING);
    assertEquals(schema.getColumn("c0").getComment(), "c0");
    assertEquals(schema.getColumn(1).getName(), "c1");
    assertEquals(schema.getColumn(1).getType(), OdpsType.BIGINT);
    assertEquals(schema.getColumn(1).getComment(), null);

    ArrayList<Column> paritionColumns = new ArrayList<Column>();
    paritionColumns.add(new Column("p0", OdpsType.STRING, "p0"));
    paritionColumns.add(new Column("p1", OdpsType.BIGINT));
    schema.setPartitionColumns(paritionColumns);

    assertEquals(schema.getPartitionColumns().size(), 2L);
    assertEquals(schema.getPartitionColumn("p0").getName(), "p0");
    assertEquals(schema.getPartitionColumn("p0").getType(), OdpsType.STRING);
    assertEquals(schema.getPartitionColumn("p0").getComment(), "p0");
    assertEquals(schema.getPartitionColumn(1).getName(), "p1");
    assertEquals(schema.getPartitionColumn(1).getType(), OdpsType.BIGINT);
    assertEquals(schema.getPartitionColumn(1).getComment(), null);
  }
}