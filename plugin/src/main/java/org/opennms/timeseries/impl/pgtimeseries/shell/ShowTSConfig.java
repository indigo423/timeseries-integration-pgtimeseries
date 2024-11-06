package org.opennms.timeseries.impl.pgtimeseries.shell;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Command(scope = "opennms-pgtimeseries", name = "show-ts-config", description = "Display information about postgresql's time-series configuration")
@Service
public class ShowTSConfig implements Action {

    @Reference
    private DataSource dataSource;

    @Override
    public Object execute() throws SQLException {
        String sql;
        DBUtils db = new DBUtils();
        ShellTable table = new ShellTable();
        table.column("Name");
        table.column("Partition Duration");
        table.column("Partition Lead Time");
        table.column("Retention");
        table.column("Compression After");
        try {
            Connection conn = dataSource.getConnection();
            db.watch(conn);
            sql = "select table_id,partition_duration,partition_lead_time,retention_duration,compression_duration from ts_config";
            PreparedStatement statement = conn.prepareStatement(sql);
            db.watch(statement);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                table.addRow().addContent(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
            }
            rs.close();
            System.out.println();
            table.print(System.out);
            System.out.println();
        }
        catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            db.cleanUp();
        }
        return null;
    }
}
