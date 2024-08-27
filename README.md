# Timeseries Integration pg_timeseries Plugin 

This plugin exposes an implementation of the TimeSeriesStorage interface.
It stores the data in a pg_timeseries table.
It can be used in OpenNMS to store and retrieve timeseries data.

## Prerequisite
* The pg_timeseries plugin must be installed in the target postgres database.
* For testing purposes you can run: ``docker run -d --name pg-timeseries -p 5432:5432 -e POSTGRES_PASSWORD=postgres quay.io/tembo/timeseries-pg:latest``
* The pg_timeseries extension depends on:
  * The [pg_cron](https://github.com/citusdata/pg_cron) extension
  * The [pg_partman](https://github.com/pgpartman/pg_partman) extension
  * The [Hydra Columnar](https://github.com/hydradatabase/hydra) extension


## Usage
### enable the Time Series Storage layer
* In opennms deploy root folder: ``echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timescale.properties``
### Compile from source
* compile: ``mvn install``
* copy the `timeseries-pgtimeseries-plugin.kar` from the `./assembly/kar/target` folder to `$OPENNMS_HOME/deploy`
###
### Activate in the Karaf shell:
  * ``ssh -p 8101 admin@localhost``
  * ``feature:install opennms-plugins-pgtimeseries-plugin``
  * The plugin will automatically create the necessary tables if they don't already exist.

## Links:
* Introduction to the Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
* pg_timeseries: https://github.com/tembo-io/pg_timeseries
