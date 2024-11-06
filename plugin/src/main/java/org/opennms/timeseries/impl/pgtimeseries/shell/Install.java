package org.opennms.timeseries.impl.pgtimeseries.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseInitializer;

import javax.sql.DataSource;
import java.sql.SQLException;

@Command(scope = "opennms-pgtimeseries", name = "install", description = "Create database infrastructure for the pg_timeseries plugin if it does not exist.")
@Service
public class Install implements Action {
    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {

        // Check Plugin
        if (!PGTimeseriesDatabaseInitializer.isPGTimeseriesExtensionInstalled()) {
            System.out.println("It looks like pg_timeseries extension is not installed. Attempting to install the extension....");
            PGTimeseriesDatabaseInitializer.installExtension();
        }

        // Check and create tables
        if (PGTimeseriesDatabaseInitializer.isPGTimeseriesTablesExisting()) {
            System.out.println("pg_timeseries tables exist. We are ready to rock 'n roll!");
        } else {
            System.out.println("pg_timeseries tables are missing. Will attempt to create them now.");
            PGTimeseriesDatabaseInitializer.createTables();
            System.out.println("pg_timeseries tables created.");
        }
        return null;
    }
}