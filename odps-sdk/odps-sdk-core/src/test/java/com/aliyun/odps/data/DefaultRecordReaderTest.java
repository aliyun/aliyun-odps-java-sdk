package com.aliyun.odps.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;

public class DefaultRecordReaderTest {
  private static final String csv = "\"t_tinyint\",\"t_smallint\",\"t_int\",\"t_bigint\",\"t_float\",\"t_double\",\"t_decimal\",\"t_timestamp\",\"t_string\",\"t_varchar\",\"t_char\",\"t_boolean\",\"t_binary\",\"t_array\",\"t_map\",\"t_struct\"\n"
      + "-95,19383,-1771229031,6948727165337235705,0.5246246,0.35642806214579226,\"241430112\",\"5635-08-13 02:05:04.275\",\"MM0NYZKU3D7F344UA8ZSASJMS3HU8LXSADDHE89ARTEFE6CQ6CWZ0WS3V8HNMOQ36K2B3VWSGEGGVPR1BRRXKC1G2B82U2NGMOGXOLKYHHB7WUOCYK66F1P3JM4H2TXMTRW45APAVSSBJWN8ZRRGGILYNJO85ARZTFOKFLPU2L45CXIFMGGPV8XREGST0GIES2E0D3XC6R941TQQJGJ51BGSNC07LS2MJ2L0BRC1A6DKKEL91X6JOUQYO34JI6I\",\"HHYG85BZ9CIE6CUVC550VZYCHNHONEDIH8BT98OT8AGYTEAC7N24Q3VEP570FMEJV6VYFR5BMO2RY016A26OBQSB6L4C6XP14CBLPIS0JOE3KC00CWD762RVFX3WW31JMYOT3I2M60VKKE7CZ3544QH4E7QO2GRYACHQLUR5T2LOO05ETK315B1NQW8WNDRXO5YF9Y6ENF0QFOFZCNK1BJ9M2GA135C9BYVECY4LR209AXKZUNNGH5THNFATVCN\",\"PTRK20F0L2EIVJFM146R2KHZZYWA9YNRIP8U30GRMKIOZQJ71AU2RMCD5HW4TH123YSG99XX56VWHEG7G0K0ZR53J1N2Y7A25MO1CMV96UHLPFMRUEVZ51K2325JQPUZFR5N0J9PSHRNYAIBMC1XRUT6SCPVITAAMQKTH5ZIGENQUUAFXP84OU1ZKRHVZULBAW8GVKT7P3N30CWUINLE1K7OBQVTBMJEINRIYUC2OVGSG68DUXKYK0BEQV4J45W\",false,\"2O3TAPVWS12V2B4S8Y00MFM9LP7G879SF697FHQS2VWFIXQEXJVONDNSV32OHL5R5RGI5PPZ5BKCOKYOWXZVR4EOLIR75N4LVY75HXDHQFWI8O6HAQLZUER0I7FDIVZBQ25OTQ9XOPXI7YCQIBGAXVF2YGYENR5WH5V8IBDK4M2URQJJ9GEVA1DZUA7JB3YIKL5NG1666ADWPENNNP1QUBTEIHZZ067ITOFJKIJE11CN5PP8FLUVA2KP15JEX02\",\"[FDCC78TC95JCYJ0WZHA5FWKL5WL849UD232D7PGU773L0W90FQJMGTHCT7JHL2BSNPYM3CBFVJJA6IZ9IIJCGEC1UW6X1VC1YFCVV36D6W1HH2RWL4BSYH1PKYD7DCC2E64CVK2C4BJ42T583DC22SLV7R98FHVANDESSHNY4ZV8KPFLH44Y0HJFA7VGLY2UV5PK9GSPRWZORJZGRY87LMRT6FPI23W4KA3SAVLS40PEV0XS4YOB39A6MZGNICQ]\",\"{MURU0R0QK5CUKWYJ9L5GCOKWPZAWKEDNKFSW70Q47C7ABM3S7QM8QAMB7PGXF46O8X2NMTKRV53HZIXX3AMSVJ6DQVIVCYJZ81L8LBWU4PWIFN2GKMSJTUMISAS297G7ZMO7OXTHSOV35OLXUUXFM8L5KI19SIX599WGVQ3P5U7JR7TJE1S28772ZYU26U67NTK3HSH5UUPDY8C2I0A1N9VY8F2N74FDO4X4N31PWPD72PH7GRJXXK3GFLI6E9H:E8O6ZLTA2TY6BMZ35CPIKC6U5OFLT96UA05KZBFCRJ64Q3JSSG3V3P2K4SAGXULVEB5U2GI7D1X0STSFPQH4QWXTL56A66WSH5DHUQL73GJAPCU4VAQULT55LQ68O12YB2KC9744DZ35105BJ2BV43V8JCJPZEKJDF8W17BYG61UCS5CO0NNAO8E0XLYJIOTBBYDR4AP41F4QDAABV35DYF73K330U7QR6YQBCDDFP30FN1FFZBMV5G3452JHRM}\",\"{c1:N7KB71DDE79E16U9ISXB5I3JE059YOS8PJEQ9P1TCK5N8T2QNB8YNOQC0Q3MX7VTIAF6S3GPZ92N4YEGVZ8XBI7W2JNY0BQP00V0BQKHGMUA2IJ7GPUJ6LYDQMT8EHVSU6ZOAKNPLS3JLX5NV7CAK3F11R6EW76BBFIYNVIP7ITJQ4CLWKCVK15ZZV91HA905K4TZPH6MXEY1DJV76U5YQQZXPI4IYADBSU6KGGYV40ZHG7RO9S67KWPLEQ1ANM, c2:6312370153366819037}\"";
  private static final TableSchema schema = new TableSchema();

  @BeforeClass
  public static void setup() {
    schema.addColumn(new Column("t_tinyint", OdpsType.TINYINT));
    schema.addColumn(new Column("t_smallint", OdpsType.SMALLINT));
    schema.addColumn(new Column("t_int", OdpsType.INT));
    schema.addColumn(new Column("t_bigint", OdpsType.BIGINT));
    schema.addColumn(new Column("t_float", OdpsType.FLOAT));
    schema.addColumn(new Column("t_double", OdpsType.DOUBLE));
    schema.addColumn(new Column("t_decimal", TypeInfoFactory.getDecimalTypeInfo(38, 18)));
    schema.addColumn(new Column("t_timestamp", OdpsType.TIMESTAMP));
    schema.addColumn(new Column("t_string", OdpsType.STRING));
    schema.addColumn(new Column("t_varchar", TypeInfoFactory.getVarcharTypeInfo(255)));
    schema.addColumn(new Column("t_char", OdpsType.STRING));
    schema.addColumn(new Column("t_boolean", OdpsType.BOOLEAN));
    schema.addColumn(new Column("t_binary", OdpsType.BINARY));
    schema.addColumn(new Column("t_array", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)));
    schema.addColumn(new Column("t_map",
                                TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING)));
    schema.addColumn(new Column("t_struct", TypeInfoFactory.getStructTypeInfo(Arrays.asList("c1", "c2"),
                                                                              Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING))));
  }

  @Test
  public void testNewTypes() {
    ByteArrayInputStream is = new ByteArrayInputStream(csv.getBytes());
    DefaultRecordReader reader = new DefaultRecordReader(is, schema);
    try {
      ArrayRecord r = (ArrayRecord) reader.read();
      Assert.assertEquals(Byte.valueOf("-95"), r.getTinyint("t_tinyint"));
      Assert.assertEquals(Short.valueOf("19383"), r.getSmallint("t_smallint"));
      Assert.assertEquals(Integer.valueOf("-1771229031"), r.getInt("t_int"));
      Assert.assertEquals(Long.valueOf("6948727165337235705"), r.getBigint("t_bigint"));
      Assert.assertEquals(Float.valueOf("0.5246246"), r.getFloat("t_float"));
      Assert.assertEquals(Double.valueOf("0.35642806214579226"), r.getDouble("t_double"));
      Assert.assertEquals(BigDecimal.valueOf(241430112), r.getDecimal("t_decimal"));
      Assert.assertEquals(Timestamp.valueOf("5635-08-13 02:05:04.275"), r.getTimestamp("t_timestamp"));
      Assert.assertEquals("MM0NYZKU3D7F344UA8ZSASJMS3HU8LXSADDHE89ARTEFE6CQ6CWZ0WS3V8HNMOQ36K2B3VWSGEGGVPR1BRRXKC1G2B82U2NGMOGXOLKYHHB7WUOCYK66F1P3JM4H2TXMTRW45APAVSSBJWN8ZRRGGILYNJO85ARZTFOKFLPU2L45CXIFMGGPV8XREGST0GIES2E0D3XC6R941TQQJGJ51BGSNC07LS2MJ2L0BRC1A6DKKEL91X6JOUQYO34JI6I", r.getString("t_string"));
      Assert.assertEquals("HHYG85BZ9CIE6CUVC550VZYCHNHONEDIH8BT98OT8AGYTEAC7N24Q3VEP570FMEJV6VYFR5BMO2RY016A26OBQSB6L4C6XP14CBLPIS0JOE3KC00CWD762RVFX3WW31JMYOT3I2M60VKKE7CZ3544QH4E7QO2GRYACHQLUR5T2LOO05ETK315B1NQW8WNDRXO5YF9Y6ENF0QFOFZCNK1BJ9M2GA135C9BYVECY4LR209AXKZUNNGH5THNFATVCN", r.getVarchar("t_varchar").getValue());
      Assert.assertEquals(false, r.getBoolean("t_boolean"));
      Assert.assertEquals("2O3TAPVWS12V2B4S8Y00MFM9LP7G879SF697FHQS2VWFIXQEXJVONDNSV32OHL5R5RGI5PPZ5BKCOKYOWXZVR4EOLIR75N4LVY75HXDHQFWI8O6HAQLZUER0I7FDIVZBQ25OTQ9XOPXI7YCQIBGAXVF2YGYENR5WH5V8IBDK4M2URQJJ9GEVA1DZUA7JB3YIKL5NG1666ADWPENNNP1QUBTEIHZZ067ITOFJKIJE11CN5PP8FLUVA2KP15JEX02", new String(r.getBinary("t_binary").data(),
                                                                                                                                                                                                                                                                                                        StandardCharsets.UTF_8));
      // TODO: support complex types
      Assert.assertNull(r.getArray("t_array"));
      Assert.assertNull(r.getMap("t_map"));
      Assert.assertNull(r.getStruct("t_struct"));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
