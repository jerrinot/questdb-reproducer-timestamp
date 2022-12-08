package io.questdb.reproducer.timestamp;

import io.questdb.client.Sender;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.QuestDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.*;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTimestamp {

    private static final long NOW = System.currentTimeMillis();

    @Rule
    public final QuestDBContainer questDbContainer = new QuestDBContainer("questdb/questdb:6.6.1")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI")
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")));

    @Test
    public void testTimestamps() throws Exception {
        try (Sender sender = Sender.builder().address(questDbContainer.getIlpUrl()).build()) {
            sender.table("test")
                    .timestampColumn("non_designated", TimeUnit.MILLISECONDS.toMicros(NOW))
                    .at(TimeUnit.MILLISECONDS.toNanos(NOW));
            sender.flush();
        }

        assertQuestState();
    }

    private void assertQuestState() throws SQLException, InterruptedException {
        try (var conn = newConnection();
             var stmt = conn.prepareStatement("select * from test")) {
            for (;;) {
                try {
                    executeQueryAndAssertTimestamp(stmt);
                    return;
                } catch (AssertionError e) {
                    // it can take a while for the data to be visible
                    Thread.sleep(1000);
                }
            }
        }
    }

    private Connection newConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        return DriverManager.getConnection(questDbContainer.getJdbcUrl(), properties);
    }

    private static void executeQueryAndAssertTimestamp(PreparedStatement stmt) throws SQLException {
        var rs = stmt.executeQuery();
        assertTrue(rs.next());
        Timestamp nonDesignated = rs.getTimestamp("non_designated", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        assertEquals(NOW, nonDesignated.getTime());
        Timestamp designated = rs.getTimestamp("timestamp", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        assertEquals(NOW, designated.getTime());
    }
}
