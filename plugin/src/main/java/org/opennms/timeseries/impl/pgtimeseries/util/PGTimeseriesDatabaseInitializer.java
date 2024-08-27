package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The pgtimeseries plugin uses the opennms database. But it needs extra tables.
 * This class offers helper methods to check for and create the tables.
 */
@AllArgsConstructor
@Slf4j
public class PGTimeseriesDatabaseInitializer {

    private final DataSource dataSource;

    boolean isPGTimeseriesExtensionInstalled() throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = dataSource.getConnection();
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);

            ResultSet result = stmt.executeQuery("select count(*) from pg_extension where extname = 'timeseries';");
            db.watch(result);
            result.next();
            return result.getInt(1) > 0;
        } finally {
            db.cleanUp();
        }
    }

    boolean isPGTimeseriesTableExisting(String tableName) throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = dataSource.getConnection();
            db.watch(conn);
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, null, tableName, null);
            db.watch(tables);
            return tables.next();
        } finally {
            db.cleanUp();
        }
    }

    boolean isPGTimeseriesTablesExisting() throws SQLException {
        return isPGTimeseriesTableExisting(TableNames.PGTIMESERIES_TIME_SERIES)
                && isPGTimeseriesTableExisting(TableNames.PGTIMESERIES_METRIC)
                && isPGTimeseriesTableExisting(TableNames.PGTIMESERIES_TAG);
    }

    void createTables() throws SQLException {
        DBUtils db = new DBUtils();
        try {
            Connection conn = dataSource.getConnection();
            db.watch(conn);
            Statement stmt = conn.createStatement();
            db.watch(stmt);
            // Create the table to hold the series
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_time_series(key TEXT NOT NULL, time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NULL) PARTITION BY RANGE (time)");
            // let pg_timseries take over the table; set partition for 1 week duration
            // TODO make this configurable
            executeQuery(stmt, "SELECT enable_ts_table('pgtimeseries_time_series', partition_duration := '1 week')");
            // set total retention for 1 year
            // TODO make this duration configurable
            executeQuery(stmt, "SELECT set_ts_retention_policy('pgtimeseries_time_series', '1 year'");
            // enable compression after 3 months
            // TODO make this duration configurable too
            executeQuery(stmt, "SELECT set_ts_compression_policy('pgtimeseries_time_series', '3 months')");
            //Indexes.  This is a WAG.
            executeQuery(stmt, "CREATE INDEX ON pgtimeseries_time_series(time DESC)");
            executeQuery(stmt, "CREATE INDEX ON pgtimeseries_time_series(key)");
            // Metrics table
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_metric(key TEXT NOT NULL PRIMARY KEY)");
            // tag table
            executeQuery(stmt, "CREATE TABLE IF NOT EXISTS pgtimeseries_tag(fk_pgtimeseries_metric TEXT NOT NULL, key TEXT, value TEXT NOT NULL, type TEXT NOT NULL, UNIQUE (fk_pgtimeseries_metric, key, value, type))");
        } finally {
            db.cleanUp();
        }
    }

    private void executeQuery(Statement stmt, final String sql) throws SQLException {
        log.info(sql);
        stmt.execute(sql);
    }

    public void initializeIfNeeded() throws SQLException {

        // Check Plugin
        if (!isPGTimeseriesExtensionInstalled()) {
            log.error("It looks like pg_timeseries extension is not installed. Please install: pg_timeseries_docs_url Aborting.");
            return;
        }

        // Check and create tables
        if (isPGTimeseriesTablesExisting()) {
            log.info("pg_timeseries tables exist. We are good to go.");
        } else {
            log.info("pg_timeseries tables are missing. Will create them now.");
            createTables();
            log.info("pg_timeseries tables created.");
        }
    }
}
