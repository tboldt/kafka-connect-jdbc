/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class Db2DialectTest extends BaseDialectTest {

  public Db2DialectTest() {
    super(new Db2Dialect());
  }

  @Test
  public void dataTypeMappingsOrganizeByRow() {
    ((Db2Dialect) dialect).setTableOrganization(Db2Dialect.Db2TableOrganization.ROW);
    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("CHAR(1)", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(31,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(31,42)", Decimal.schema(42));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void dataTypeMappingsOrganizeByColumn() {
    ((Db2Dialect) dialect).setTableOrganization(Db2Dialect.Db2TableOrganization.COLUMN);
    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("CHAR(1)", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(32592)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARCHAR(32592) FOR BIT DATA", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(31,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(31,42)", Decimal.schema(42));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"test\" ADD \"newcol1\" INTEGER NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"test\" " + System.lineSeparator()
        + "ADD \"newcol1\" INTEGER NULL," + System.lineSeparator()
        + "ADD \"newcol2\" INTEGER DEFAULT 42"
    );
  }

  @Test
  public void upsert() {
    assertEquals(
        "merge into \"ARTICLE\" " +
        "using (select ? \"title\", ? \"author\", ? \"body\" FROM SYSIBM.SYSDUMMY1) incoming on" +
        "(\"ARTICLE\".\"title\"=incoming.\"title\" and \"ARTICLE\".\"author\"=incoming.\"author\") " +
        "when matched then update set \"ARTICLE\".\"body\"=incoming.\"body\" " +
        "when not matched then insert(\"ARTICLE\".\"body\",\"ARTICLE\".\"title\",\"ARTICLE\".\"author\") " +
        "values(incoming.\"body\",incoming.\"title\",incoming.\"author\")",
        dialect.getUpsertQuery("ARTICLE", Arrays.asList("title", "author"), Collections.singletonList("body"))
    );
  }

}