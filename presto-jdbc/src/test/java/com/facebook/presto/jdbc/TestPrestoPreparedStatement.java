/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.jdbc;

import com.facebook.presto.plugin.blackhole.BlackHolePlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import static com.facebook.presto.jdbc.TestPrestoDriver.closeQuietly;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoPreparedStatement
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = new TestingPrestoServer();

        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        waitForNodeRefresh(server);
        setupTestTables();
    }

    private static void waitForNodeRefresh(TestingPrestoServer server)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (server.refreshNodes().getActiveNodes().size() < 1) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }

    private void setupTestTables()
            throws SQLException
    {
        try (Connection connection = createConnection("blackhole", "blackhole");
                Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate("CREATE SCHEMA blackhole.blackhole"), 0);

            try (Statement st = connection.createStatement()) {
                st.execute("CREATE TABLE test_execute_update (" +
                        "c_null boolean, " +
                        "c_boolean boolean, " +
                        "c_integer integer, " +
                        "c_bigint bigint, " +
                        "c_real real, " +
                        "c_double double, " +
                        "c_decimal decimal, " +
                        "c_varchar varchar, " +
                        "c_date date, " +
                        "c_time time, " +
                        "c_timestamp timestamp" +
                        ")");
            }

            try (Statement st = connection.createStatement()) {
                st.execute("CREATE TABLE test_execute_simple_update (c_integer integer)");
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
    }

    @Test
    public void testExecute()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.setShort(1, (short) 3);
                assertTrue(statement.execute());
            }

            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test_execute_simple_update VALUES (?)")) {
                statement.setInt(1, 1);
                assertFalse(statement.execute());
            }
        }
    }

    @Test
    public void testExecuteWithoutParameters()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT 1")) {
                assertTrue(statement.execute());
            }
        }
    }

    @Test
    public void testGetResultSet()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.setInt(1, 100);
                statement.execute();
                ResultSet rs = statement.getResultSet();

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 100);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testExecuteQuery()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")) {
                statement.setNull(1, Types.VARCHAR);
                statement.setBoolean(2, true);
                statement.setShort(3, (short) 3);
                statement.setInt(4, 4);
                statement.setLong(5, 5L);
                statement.setFloat(6, 6f);
                statement.setDouble(7, 7d);
                statement.setBigDecimal(8, BigDecimal.valueOf(8L));
                statement.setString(9, "9'9");
                statement.setDate(10, new Date(10));
                statement.setTime(11, new Time(11));
                statement.setTimestamp(12, new Timestamp(12));
                ResultSet rs = statement.executeQuery();
                assertTrue(rs.next());

                assertEquals(rs.getObject(1), null);
                assertEquals(rs.getBoolean(2), true);
                assertEquals(rs.getShort(3), (short) 3);
                assertEquals(rs.getInt(4), 4);
                assertEquals(rs.getLong(5), 5L);
                assertEquals(rs.getFloat(6), 6f);
                assertEquals(rs.getDouble(7), 7d);
                assertEquals(rs.getBigDecimal(8), BigDecimal.valueOf(8L));
                assertEquals(rs.getString(9), "9'9");
                assertEquals(rs.getDate(10).toString(), new Date(10).toString());
                assertEquals(rs.getTime(11).toString(), new Time(11).toString());
                assertEquals(rs.getTimestamp(12), new Timestamp(12));
                assertFalse(rs.next());
            }
        }
    }

    @Test(expectedExceptions = {SQLException.class})
    public void testExecuteQueryShouldThrowExceptionIfItIsNotAQuery()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test_execute_simple_update VALUES (5)")) {
                statement.executeQuery();
            }
        }
    }

    @Test
    public void testExecuteUpdate()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test_execute_update VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                statement.setNull(1, Types.BOOLEAN);
                statement.setBoolean(2, true);
                statement.setInt(3, 3);
                statement.setLong(4, 4);
                statement.setFloat(5, 5f);
                statement.setDouble(6, 6d);
                statement.setBigDecimal(7, BigDecimal.valueOf(7L));
                statement.setString(8, "8'8");
                statement.setDate(9, new Date(9));
                statement.setTime(10, new Time(10));
                statement.setTimestamp(11, new Timestamp(11));
                assertEquals(statement.executeUpdate(), 1);
            }
        }
    }

    @Test
    public void testExecuteLargeUpdate()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO test_execute_update VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                statement.setNull(1, Types.BOOLEAN);
                statement.setBoolean(2, true);
                statement.setInt(3, 3);
                statement.setLong(4, 4);
                statement.setFloat(5, 5f);
                statement.setDouble(6, 6d);
                statement.setBigDecimal(7, BigDecimal.valueOf(7L));
                statement.setString(8, "8'8");
                statement.setDate(9, new Date(9));
                statement.setTime(10, new Time(10));
                statement.setTimestamp(11, new Timestamp(11));
                assertEquals(statement.executeLargeUpdate(), 1);
            }
        }
    }

    @Test(expectedExceptions = {SQLException.class})
    public void testExecuteUpdateShouldThrowExceptionIfItIsNotAnUpdate()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.executeUpdate();
            }
        }
    }

    @Test
    public void testSubstitutionParamsSeveralTimes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.setBoolean(1, true);
                ResultSet rs1 = statement.executeQuery();
                assertTrue(rs1.next());
                assertTrue(rs1.getBoolean(1));

                statement.setString(1, "foo");
                ResultSet rs2 = statement.executeQuery();

                assertTrue(rs2.next());
                assertEquals(rs2.getString(1), "foo");
            }
        }
    }

    @Test
    public void testCleaningUpResources()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs1;
            ResultSet rs2;

            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.setNull(1, Types.VARCHAR);
                rs1 = statement.executeQuery();
                assertFalse(rs1.isClosed());

                rs2 = statement.executeQuery();
                assertTrue(rs1.isClosed());
                assertFalse(rs2.isClosed());
            }
            assertTrue(rs2.isClosed());
        }
    }

    @Test
    public void testGetMetadata()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT 1 as intval, ? as unknownval")) {
                ResultSetMetaData metadata = statement.getMetaData();
                assertEquals(metadata.getColumnCount(), 2);

                assertEquals(metadata.getCatalogName(1), "");
                assertEquals(metadata.getSchemaName(1), "");
                assertEquals(metadata.getTableName(1), "");
                assertEquals(metadata.getColumnClassName(1), "java.lang.Integer");
                assertEquals(metadata.getColumnDisplaySize(1), 11);
                assertEquals(metadata.getColumnLabel(1), "intval");
                assertEquals(metadata.getColumnName(1), "intval");
                assertEquals(metadata.getColumnType(1), Types.INTEGER);
                assertEquals(metadata.getColumnTypeName(1), "integer");
                assertEquals(metadata.getPrecision(1), 10);
                assertEquals(metadata.getScale(1), 0);
                assertEquals(metadata.isSigned(1), true);

                assertEquals(metadata.getColumnLabel(2), "unknownval");
                assertEquals(metadata.getColumnName(2), "unknownval");
                assertEquals(metadata.getColumnTypeName(2), "unknown");
            }
        }
    }

    @Test
    public void testGetParameterMetadata()
            throws Exception
    {
        try (Connection connection = createConnection("blackhole", "blackhole")) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT *, ? as unknownval FROM test_execute_update WHERE c_boolean = ?")) {
                ParameterMetaData metadata = statement.getParameterMetaData();
                assertEquals(metadata.getParameterCount(), 2);

                assertEquals(metadata.getParameterTypeName(1), "unknown");

                assertEquals(metadata.getParameterTypeName(2), "boolean");
                assertEquals(metadata.getParameterType(2), Types.BOOLEAN);
                assertEquals(metadata.getParameterClassName(2), "java.lang.Boolean");
                assertEquals(metadata.getParameterMode(2), ParameterMetaData.parameterModeIn);
                assertEquals(metadata.getPrecision(2), 0);
                assertEquals(metadata.getScale(2), 0);
                assertEquals(metadata.isSigned(2), false);
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection(format("jdbc:presto://%s", server.getAddress()));
    }

    private Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        return createConnection(format("jdbc:presto://%s/%s/%s", server.getAddress(), catalog, schema));
    }

    private Connection createConnection(String url)
            throws SQLException
    {
        return DriverManager.getConnection(url, "test", null);
    }
}
