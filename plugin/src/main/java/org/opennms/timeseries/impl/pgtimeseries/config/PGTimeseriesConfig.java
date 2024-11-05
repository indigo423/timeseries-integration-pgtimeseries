package org.opennms.timeseries.impl.pgtimeseries.config;

import java.util.Objects;
import java.util.StringJoiner;

public class PGTimeseriesConfig {

    private final String externalDatasourceURL;
    private final String adminDatasourceURL;
    private final String partitionDuration;
    private final String retentionPolicy;
    private final String compressionPolicy;
    private final String backfillStart;
    private final boolean createTablesOnInstall;

    public PGTimeseriesConfig() {
        this(builder());
    }

    public PGTimeseriesConfig(Builder builder) {
        this.externalDatasourceURL = builder.externalDatasourceURL;
        this.adminDatasourceURL = builder.adminDatasourceURL;
        this.partitionDuration = builder.partitionDuration;
        this.retentionPolicy = builder.retentionPolicy;
        this.compressionPolicy = builder.compressionPolicy;
        this.backfillStart = builder.backfillStart;
        this.createTablesOnInstall = builder.createTablesOnInstall;
    }

    /** Will be called via blueprint. The builder can be called when not running as Osgi plugin. */
    public PGTimeseriesConfig(
            final String externalDatasourceURL,
            final String adminDatasourceURL,
            final String partitionDuration,
            final String retentionPolicy,
            final String compressionPolicy,
            final String backfillStart,
            final boolean createTablesOnInstall) {
        this(builder()
                .externalDatasourceURL(externalDatasourceURL)
                .adminDatasourceURL(adminDatasourceURL)
                .partitionDuration(partitionDuration)
                .retentionPolicy(retentionPolicy)
                .compressionPolicy(compressionPolicy)
                .backfillStart(backfillStart)
                .createTablesOnInstall(createTablesOnInstall));
    }

    public String getExternalDatasourceURL() {
        return externalDatasourceURL;
    }

    public String getAdminDatasourceURL() {
        return adminDatasourceURL;
    }

    public String getpartitionDuration() {
        return partitionDuration;
    }

    public String getretentionPolicy() {
        return retentionPolicy;
    }

    public String getcompressionPolicy() {
        return compressionPolicy;
    }

    public String getbackfillStart() {
        return backfillStart;
    }

    public boolean createTablesOnInstall() {
        return createTablesOnInstall;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {
        private String externalDatasourceURL = "jdbc:postgresql://localhost:5432/opennms?user=opennms&password=opennms";
        private String adminDatasourceURL = "jdbc:postgresql://localhost:5432/opennms?user=postgres&password=opennms";
        private String partitionDuration = "1 week";
        private String retentionPolicy = "1 year";
        private String compressionPolicy = "3 months";
        private String backfillStart = null;
        private boolean createTablesOnInstall = true;

        public Builder externalDatasourceURL(final String externalDatasourceURL) {
            this.externalDatasourceURL = externalDatasourceURL;
            return this;
        }

        public Builder adminDatasourceURL(final String adminDatasourceURL) {
            this.adminDatasourceURL = adminDatasourceURL;
            return this;
        }

        public Builder partitionDuration(final String partitionDuration) {
            this.partitionDuration = partitionDuration;
            return this;
        }

        public Builder retentionPolicy(final String retentionPolicy) {
            this.retentionPolicy = retentionPolicy;
            return this;
        }

        public Builder compressionPolicy(final String compressionPolicy) {
            this.compressionPolicy = compressionPolicy;
            return this;
        }

        public Builder backfillStart(final String backfillStart) {
            this.backfillStart = backfillStart;
            return this;
        }

        public Builder createTablesOnInstall(final boolean createTablesOnInstall) {
            this.createTablesOnInstall = createTablesOnInstall;
            return this;
        }

        public PGTimeseriesConfig build() {
            return new PGTimeseriesConfig(this);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PGTimeseriesConfig.class.getSimpleName() + "[", "]")
                .add("externalDatasourceURL='" + externalDatasourceURL + "'")
                .add("adminDatasourceURL='" + adminDatasourceURL + "'")
                .add("partitionDuration='" + partitionDuration + "'")
                .add("retentionPolicy='" + retentionPolicy + "'")
                .add("compressionPolicy='" + compressionPolicy + "'")
                .add("backfillStart='" + backfillStart + "'")
                .add("createTablesOnInstall='" + createTablesOnInstall + "'")
                .toString();
    }
}