package com.aliyun.odps.sqa.commandapi.utils;

import java.sql.SQLException;

import org.junit.Test;

public class SqlParseUtilTest {

  @Test
  public void hasResultSet() throws SQLException {
    String[] strArray = {"create table sale_detail_insert like sale_detail;",

                         "alter table sale_detail_insert add partition(sale_date='2013', region='china');",

                         "select shop_name, customer_id, total_price from sale_detail;",

                         "from sale_detail\n"
                         + "        insert overwrite table sale_detail_multi partition (sale_date='2010', region='china' ) \n"
                         + "            select shop_name, customer_id, total_price \n"
                         + "        insert overwrite table sale_detail_multi partition (sale_date='2011', region='china' )\n"
                         + "            select shop_name, customer_id, total_price ;",

                         "from sale_detail\n"
                         + "        insert overwrite table sale_detail_multi partition (sale_date='2010', region='china' ) \n"
                         + "            select shop_name, customer_id, total_price;",

                         "from table1\n" + " select a;",

                         "insert into table srcp partition (p)(key,p) values ('d','20170101'),('e','20170101'),('f','20170101');",

                         "insert into table srcp partition (p) select concat(a,b), length(a)+length(b),'20170102' from  values ('d',4),('e',5),('f',6) t(a,b);",

                         "select region from sale_detail group by region;",

                         " select region from sale_detail distribute by region;",

                         "  SELECT * FROM VALUES (1, 2), (1, 2), (3, 4), (5, 6) t(a, b) \n"
                         + "  INTERSECT ALL \n"
                         + "  SELECT * FROM VALUES (1, 2), (1, 2), (3, 4), (5, 7) t(a, b);",

                         "select a.shop_name as ashop, b.shop_name as bshop from shop a left outer join sale_detail b on a.shop_name=b.shop_name;",

                         "delete from acid_delete where id = 2; ",

                         "update acid_update set id = 4 where id = 2; ",

                         "merge into acid_address_book_base1 as t using tmp_table1 as s \n"
                         + "on s.id = t.id and t.year='2020' and t.month='08' and t.day='20' and t.hour='16' \n"
                         + "when matched \n"
                         + "then update set t.first_name = s.first_name, t.last_name = s.last_name, t.phone = s.phone \n"
                         + "when not matched and (s._event_type_='I') \n"
                         + "then insert values(s.id, s.first_name, s.last_name,s.phone,'2020','08','20','16');",

                         "with a as (select a from table)" + "select * from a;",

                         "explain select a from tunnel_updown_1;"};

    for (String s : strArray) {
      System.out.print(s);
      System.out.print(" has resultSet? ");
      System.out.print(SqlParserUtil.hasResultSet(s));
      System.out.println();
    }

    System.out.println();

    for (String s : strArray) {
      System.out.print(s);
      System.out.print(" is select? ");
      System.out.print(SqlParserUtil.isSelect(s));
      System.out.println();
    }
  }
}