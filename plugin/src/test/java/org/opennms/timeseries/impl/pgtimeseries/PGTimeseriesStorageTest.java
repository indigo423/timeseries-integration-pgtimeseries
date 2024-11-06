
package org.opennms.timeseries.impl.pgtimeseries;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opennms.integration.api.v1.timeseries.AbstractStorageIntegrationTest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PGTimeseriesStorageTest extends AbstractStorageIntegrationTest {


    public static GenericContainer<?> container;

    private DataSource dataSource;
    private PGTimeseriesStorage pgtimeseries;

    @BeforeClass
    public static void setUpContainer() {
        container = new GenericContainer<>("pgtimeseries/pgtimeseriesdb:latest-pg12")
                .withExposedPorts(5432)
                .withEnv("POSTGRES_PASSWORD", "password")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
                .withLogConsumer(new Slf4jLogConsumer(log));
        container.start();
    }

    @AfterClass
    public static void tearDownContainer() {
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
    public void setUp() throws Exception {
        dataSource = createDatasource();
        pgtimeseries = new PGTimeseriesStorage(PGTimeseriesConfig.builder().build(), dataSource);
        pgtimeseries.init();
        super.setUp();
    }

    @After
    public void tearDown() throws SQLException {
        dropTables(dataSource);
    }

    @Override
    protected TimeSeriesStorage createStorage() {
        return pgtimeseries;
    }

    private void dropTables(final DataSource dataSource) throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = dataSource.getConnection();
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);
            stmt.execute("DROP TABLE pgtimeseries_tag;");
            stmt.execute("DROP TABLE pgtimeseries_metric;");
            stmt.execute("DROP TABLE pgtimeseries_time_series;");
        } finally {
            db.cleanUp();
        }
    }
}
