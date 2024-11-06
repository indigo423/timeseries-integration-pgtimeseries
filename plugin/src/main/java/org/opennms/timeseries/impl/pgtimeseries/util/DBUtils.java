
package org.opennms.timeseries.impl.pgtimeseries.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for tracking open JDBC {@link Statement}s, {@link ResultSet}s, and {@link Connection}s
 * to ease cleanup and avoid connection leaks.
 */

public class DBUtils {
    private Logger LOG;

    private final Set<Statement> statements;
    private final Set<ResultSet> resultSets;
    private final Set<Connection> connections;

    /**
     * <p>Constructor for DBUtils.</p>
     */
    public DBUtils() {
        this(DBUtils.class);
    }

    /**
     * <p>Constructor for DBUtils.</p>
     *
     * @param loggingClass a {@link java.lang.Class} object.
     */
    public DBUtils(Class<?> loggingClass) {
        statements = Collections.synchronizedSet(new HashSet<Statement>());
        resultSets = Collections.synchronizedSet(new HashSet<ResultSet>());
        connections = Collections.synchronizedSet(new HashSet<Connection>());
        LOG = LoggerFactory.getLogger(loggingClass);
    }

    public DBUtils(Class<?> loggingClass, Object... targets) {
        this(loggingClass);
        for (final Object o : targets) {
            watch(o);
        }
    }

    public DBUtils setLoggingClass(Class<?> c) {
        LOG = LoggerFactory.getLogger(c);
        return this;
    }

    public DBUtils watch(Object o) {
        if (o instanceof Statement) {
            statements.add((Statement)o);
        } else if (o instanceof ResultSet) {
            resultSets.add((ResultSet)o);
        } else if (o instanceof Connection) {
            connections.add((Connection)o);
        } else {
            LOG.debug("Unknown object {}", o.toString());
        }
        return this;
    }

    public void cleanUp() {
        for (ResultSet rs : resultSets) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Throwable e) {
                    LOG.warn("Unable to close result set", e);
                }
            }
        }
        resultSets.clear();

        for (Statement s : statements) {
            if (s != null) {
                try {
                    s.close();
                } catch (Throwable e) {
                    LOG.warn("Unable to close statement", e);
                }
            }
        }
        statements.clear();

        for (Connection c : connections) {
            if (c != null) {
                try {
                    LOG.trace("Closing connection: " + c.toString());
                    c.close();
                } catch (Throwable e) {
                    LOG.warn("Unable to close connection", e);
                }
            }
        }
        connections.clear();
    }

}
