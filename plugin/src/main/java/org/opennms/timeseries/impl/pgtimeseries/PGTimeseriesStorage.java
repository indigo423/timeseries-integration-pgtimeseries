
package org.opennms.timeseries.impl.pgtimeseries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.MetaTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;
import org.opennms.timeseries.impl.pgtimeseries.config.PGTimeseriesConfig;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseInitializer;
import org.slf4j.Logger;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PGTimeseriesStorage implements TimeSeriesStorage {

    private static final Logger RATE_LIMITED_LOGGER = log;

    private final DataSource dataSource;
    private final PGTimeseriesConfig config;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");
    private final Meter samplesRead = metrics.meter("samplesRead");
    private final Meter samplesLost = metrics.meter("samplesLost");
    final JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("org.opennms.timeseries.impl.pgtimeseries").build();

    public PGTimeseriesStorage(final PGTimeseriesConfig config, final DataSource dataSource) {
        this.config = Objects.requireNonNull(config);
        this.dataSource = Objects.requireNonNull(dataSource);
    }
    
    @Override
    public void store(List<Sample> entries) throws StorageException {
        String sql = "INSERT INTO pgtimeseries_time_series(time, key, value)  values (?, ?, ?)";

        final DBUtils db = new DBUtils(this.getClass());
        int batchSize = 0;
        Connection connection = null;
        try {
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                log.trace("Store got connection: " + connection.toString());
            } else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);
            PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            // Partition the samples into collections smaller than max_batch_size
            for (List<Sample> batch : Lists.partition(entries, config.getMaxBatchSize())) {
                log.debug("Inserting {} samples", batch.size());
                for (Sample sample : batch) {
                    ps.setTimestamp(1, new Timestamp(sample.getTime().toEpochMilli()));
                    ps.setString(2, sample.getMetric().getKey());
                    ps.setDouble(3, sample.getValue());
                    ps.addBatch();
                    storeTags(connection, sample.getMetric(), ImmutableMetric.TagType.intrinsic, sample.getMetric().getIntrinsicTags());
                    storeTags(connection, sample.getMetric(), ImmutableMetric.TagType.meta, sample.getMetric().getMetaTags());
                    storeTags(connection, sample.getMetric(), ImmutableMetric.TagType.external, sample.getMetric().getExternalTags());
                }
                batchSize = batch.size();
                ps.executeBatch();
                samplesWritten.mark(batchSize);

                if (log.isDebugEnabled()) {
                    String keys = batch.stream()
                            .map(s -> s.getMetric().getKey())
                            .distinct()
                            .collect(Collectors.joining(", "));
                    log.debug("Successfully inserted samples for resources with ids {}", keys);
                }
            }
        } catch (SQLException e) {
            RATE_LIMITED_LOGGER.error("An error occurred while inserting samples. Some sample may be lost.", e);
            samplesLost.mark(batchSize);
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    private void storeTags(final Connection connection, final Metric metric, final ImmutableMetric.TagType tagType, final Collection<Tag> tags) throws SQLException {
        final String sql = "INSERT INTO pgtimeseries_tag(fk_pgtimeseries_metric, key, value, type)  values (?, ?, ?, ?) ON CONFLICT (fk_pgtimeseries_metric, key, value, type) DO NOTHING;";
        final DBUtils db = new DBUtils(this.getClass());
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            for (Tag tag : tags) {
                ps.setString(1, metric.getKey());
                ps.setString(2, tag.getKey());
                ps.setString(3, tag.getValue());
                ps.setString(4, tagType.name());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } finally {
            db.cleanUp();
        }
    }

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> matchers) throws StorageException {
        Objects.requireNonNull(matchers, "tags collection can not be null");
        if (matchers.isEmpty()) {
            throw new IllegalArgumentException("Collection<TagMatcher> can not be empty");
        }

        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        try {

            String sql = createMetricsSQL(matchers);
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                log.trace("FindMetrics got connection: " + connection.toString());
            }
            else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);

            // Get all relevant metricKeys
            PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            ResultSet rs = ps.executeQuery();
            db.watch(rs);
            Set<String> metricKeys = new HashSet<>();
            while (rs.next()) {
                metricKeys.add(rs.getString("fk_pgtimeseries_metric"));
            }
            rs.close();

            // Load the actual metrics
            return loadMetrics(connection, db, metricKeys);
        } catch (SQLException e) {
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    private List<Metric> loadMetrics(Connection connection, DBUtils db, Collection<String> metricKeys) throws SQLException {
        List<Metric> metrics = new ArrayList<>();
        String sql = "SELECT * FROM pgtimeseries_tag WHERE fk_pgtimeseries_metric=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        db.watch(ps);
        for (String metricKey : metricKeys) {
            ps.setString(1, metricKey);
            ResultSet rs = ps.executeQuery();
            db.watch(rs);
            ImmutableMetric.MetricBuilder metric = ImmutableMetric.builder();
            boolean intrinsicTagAvailable = false;
            while (rs.next()) {
                Tag tag = new ImmutableTag(rs.getString("key"), rs.getString("value"));
                ImmutableMetric.TagType type = ImmutableMetric.TagType.valueOf(rs.getString("type"));
                if ((type == ImmutableMetric.TagType.intrinsic)) {
                    metric.intrinsicTag(tag);
                    intrinsicTagAvailable = true;
                } else if (type == ImmutableMetric.TagType.meta) {
                    metric.metaTag(tag);
                } else if (type == ImmutableMetric.TagType.external) {
                    metric.externalTag(tag);
                } else {
                    throw new IllegalArgumentException("Unknown ImmutableMetric.TagType " + type);
                }
            }
            if (intrinsicTagAvailable) {
                // create metric only if at least one intrinsic tag is available. Otherwise we are no valid metric.
                metrics.add(metric.build());
            }
            rs.close();
        }
        return metrics;
    }

    String createMetricsSQL(Collection<TagMatcher> matchers) {
        Objects.requireNonNull(matchers, "matchers collection can not be null");
        List<TagMatcher> tagsList = new ArrayList<>(matchers);
        StringBuilder b = new StringBuilder("select distinct t0.fk_pgtimeseries_metric from pgtimeseries_tag t0");
        for (int i = 1; i < matchers.size(); i++) {
            b.append(String.format(" join pgtimeseries_tag t%s on t%s.fk_pgtimeseries_metric = t%s.fk_pgtimeseries_metric", i, i - 1, 1));
        }
        b.append(" WHERE");
        for (int i = 0; i < matchers.size(); i++) {
            if (i > 0) {
                b.append(" AND");
            }
            TagMatcher matcher = tagsList.get(i);
            String comp = tagMatcherToComp(matcher);
            b.append(String.format(" (t%s.key='%s' AND t%s.value %s '%s')", i, matcher.getKey(), i, comp, matcher.getValue()));
        }
        b.append(";");

        return b.toString();
    }

    private String tagMatcherToComp(final TagMatcher matcher) {
        // see https://www.postgresql.org/docs/11/functions-matching.html#FUNCTIONS-POSIX-REGEXP
        Objects.requireNonNull(matcher);
        switch (matcher.getType()) {
            case EQUALS:
                return "=";
            case NOT_EQUALS:
                return "!=";
            case EQUALS_REGEX:
                return "~";
            case NOT_EQUALS_REGEX:
                return "!~";
            default:
                throw new IllegalArgumentException("Unknown TagMatcher.Type " + matcher.getType().name());
        }
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) throws StorageException {

        DBUtils db = new DBUtils();
        ArrayList<Sample> samples;
        Connection connection = null;
        try {
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                log.trace("getTimeseries got connection: " + connection.toString());
            }
            else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);
            long stepInSeconds = request.getStep().getSeconds();
            List<Metric> metrics = loadMetrics(connection, db, Collections.singletonList(request.getMetric().getKey()));
            if (metrics.isEmpty()) {
                // we didn't find teh metric => nothing to do.
                return Collections.emptyList();
            }
            Metric metric = metrics.get(0);

            String sql;
            Timestamp start = new java.sql.Timestamp(request.getStart().toEpochMilli());
            Timestamp end = new java.sql.Timestamp(request.getEnd().toEpochMilli());
            String type = metric.getFirstTagByKey(MetaTagNames.mtype).getValue();
            String aggr;
            if (Metric.Mtype.count.name().equals(type) || Metric.Mtype.counter.name().equals(type)) {
                // This is a counter
                if (Aggregation.NONE == request.getAggregation()) {
                    aggr = "sum";
                } else {
                    aggr = toSql(request.getAggregation());
                }
                sql = String.format("WITH intervals AS ( " +
                "SELECT " +
                    "n AS start_time, " +
                    "n + '%s seconds'::pg_catalog.interval AS end_time " +
                "FROM " +
                    "generate_series( '%s', '%s', '%s seconds'::pg_catalog.interval ) AS n " +
            "), raw_values AS ( " +
                "SELECT " +
                    "time AS step, " +
                    "value - lag( value ) OVER ( ORDER BY time ) AS deltaval " +
                "FROM " +
                    "pgtimeseries_time_series " +
                "WHERE " +
                    "key = ? AND " +
                    "time > '%s' AND " +
                    "time < '%s' " +
            ") " +
            "SELECT " +
                "f.start_time AS step, " +
                "COALESCE( " +
                    "%s( deltaval ) / %s, " +
                    "'NaN' " +
                ") as aggregation " +
            "FROM " +
                "raw_values AS r " +
                "RIGHT JOIN intervals AS f ON r.step >= f.start_time AND " +
                "r.step < f.end_time " +
            "GROUP BY " +
                "f.start_time, " +
                "f.end_time " +
            "ORDER BY " +
                "f.start_time", stepInSeconds, start, end, stepInSeconds, start, end, aggr, stepInSeconds);
            } else { // Not a counter!
                if (Aggregation.NONE == request.getAggregation()) {
                    sql = String.format("WITH intervals AS ( " +
                    "SELECT " +
                        "n AS start_time, " +
                        "n + '%s seconds'::pg_catalog.interval AS end_time " +
                    "FROM " +
                        "generate_series( '%s', '%s', '%s seconds'::pg_catalog.interval ) AS n " +
                "), raw_values AS ( " +
                    "SELECT " +
                        "time AS step, " +
                        "value AS raw_value " +
                    "FROM " +
                        "pgtimeseries_time_series " +
                    "WHERE " +
                        "key = ? AND " +
                        "time > '%s' AND " +
                        "time < '%s' " +
                ") " +
                "SELECT " +
                    "f.start_time AS step, " +
                    "COALESCE( " +
                        "raw_value, " +
                        "'NaN' " +
                    ") as aggregation " +
                "FROM " +
                    "raw_values AS r " +
                    "RIGHT JOIN intervals AS f ON r.step >= f.start_time AND " +
                    "r.step < f.end_time " +
                "GROUP BY " +
                    "f.start_time, " +
                    "f.end_time " +
                "ORDER BY " +
                    "f.start_time", stepInSeconds, start, end, stepInSeconds, start, end);
                } else {
                    sql = String.format("WITH intervals AS ( " +
                    "SELECT " +
                        "n AS start_time, " +
                        "n + '%s seconds'::pg_catalog.interval AS end_time " +
                    "FROM " +
                        "generate_series( '%s', '%s', '%s seconds'::pg_catalog.interval ) AS n " +
                "), raw_values AS ( " +
                    "SELECT " +
                        "time AS step, " +
                        "value AS raw_value " +
                    "FROM " +
                        "pgtimeseries_time_series " +
                    "WHERE " +
                        "key = ? AND " +
                        "time > '%s' AND " +
                        "time < '%s' " +
                ") " +
                "SELECT " +
                    "f.start_time AS step, " +
                    "COALESCE( " +
                        "%s( raw_value ), " +
                        "'NaN' " +
                    ") as aggregation " +
                "FROM " +
                    "raw_values AS r " +
                    "RIGHT JOIN intervals AS f ON r.step >= f.start_time AND " +
                    "r.step < f.end_time " +
                "GROUP BY " +
                    "f.start_time, " +
                    "f.end_time " +
                "ORDER BY " +
                    "f.start_time", stepInSeconds, start, end, stepInSeconds, start, end, toSql(request.getAggregation()));
                }
            }
            PreparedStatement statement = connection.prepareStatement(sql);
            db.watch(statement);

            statement.setString(1, request.getMetric().getKey());
            ResultSet rs = statement.executeQuery();
            db.watch(rs);
            samples = new ArrayList<>();
            while (rs.next()) {
                long timestamp = rs.getTimestamp("step").getTime();
                samples.add(ImmutableSample.builder().metric(metric).time(Instant.ofEpochMilli(timestamp)).value(rs.getDouble("aggregation")).build());
                samplesRead.mark();
            }
        } catch (SQLException e) {
            log.error("Could not retrieve FetchResults", e);
            throw new StorageException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            db.cleanUp();
        }
        return samples;
    }

    @Override
    public void delete(final Metric metric) throws StorageException {

        DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        try {
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                log.trace("Delete got connection: " + connection.toString());
            }
            else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);

            PreparedStatement statement = connection.prepareStatement("DELETE FROM pgtimeseries_time_series where key=?");
            db.watch(statement);
            statement.setString(1, metric.getKey());
            int deletedTimeseriesEntries = statement.executeUpdate();

            statement = connection.prepareStatement("DELETE FROM pgtimeseries_tag where fk_pgtimeseries_metric=?");
            db.watch(statement);
            statement.setString(1, metric.getKey());
            int deletedTimeseriesTags = statement.executeUpdate();

            log.debug("Deleted {} timeseries entries and {} timeseries tags for metric {}", deletedTimeseriesEntries, deletedTimeseriesTags, metric);
        } catch (SQLException e) {
            log.error("Could not retrieve FetchResults", e);
            db.cleanUp();
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
    }

    private String toSql(final Aggregation aggregation) {
        if (Aggregation.AVERAGE == aggregation) {
            return "avg";
        } else if (Aggregation.MAX == aggregation) {
            return "max";
        } else if (Aggregation.MIN == aggregation) {
            return "min";
        } else {
            throw new IllegalArgumentException("Unknown aggregation " + aggregation);
        }
    }

    @Override
    public boolean supportsAggregation(final Aggregation aggregation) {
        return aggregation == Aggregation.MAX || aggregation == Aggregation.MIN || aggregation == Aggregation.AVERAGE;
    }

    public void init() throws StorageException {
        try {
            new PGTimeseriesDatabaseInitializer(this.dataSource, this.config)
                    .initializeIfNeeded();
        } catch (final SQLException e) {
            throw new StorageException(e);
        }
        reporter.start();
    }

    public void destroy() {
        reporter.stop();
        PGTimeseriesDatabaseInitializer.ExtReporter.stop(); // janky
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }
}
