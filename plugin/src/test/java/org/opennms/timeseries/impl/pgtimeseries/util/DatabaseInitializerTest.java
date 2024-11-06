package org.opennms.timeseries.impl.pgtimeseries.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabaseInitializerTest {
    public static GenericContainer<?> container;

    private PGTimeseriesDatabaseInitializer initializer;

    @BeforeClass
    public static void setUpContainer() {
        container = new GenericContainer<>("timescale/timescaledb:latest-pg12")
                .withExposedPorts(5432)
                .withEnv("POSTGRES_PASSWORD", "password")
                .withEnv("PGTIMESERIESDB_TELEMETRY", "off")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
                .withLogConsumer(new Slf4jLogConsumer(log));

        container.start();
    }

    @AfterClass
    public static void tearDown() {
        container.stop();
    }

    private static DataSource createDatasource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://localhost:%s/", container.getFirstMappedPort()));
        config.setUsername("postgres");
        config.setPassword("password");
        return new HikariDataSource(config);
    }

    @Before
    public void setUp() {
        this.initializer = new PGTimeseriesDatabaseInitializer(createDatasource(), PGTimeseriesConfig.builder().build());
    }

    @Test
    public void shouldCreateTables() throws SQLException {
        assertTrue(initializer.isPGTimeseriesExtensionInstalled());

        assertFalse(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TIME_SERIES));
        assertFalse(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_METRIC));
        assertFalse(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TAG));

        initializer.createTables();

        assertTrue(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TIME_SERIES));
        assertTrue(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_METRIC));
        assertTrue(initializer.doesPGTimeseriesTableExist(TableNames.PGTIMESERIES_TAG));
    }
}
