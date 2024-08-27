# Timeseries Integration pg_timeseriesPlugin [![CircleCI](https://circleci.com/gh/opennms-forge/timeseries-integration-timescale.svg?style=svg)](https://circleci.com/gh/opennms-forge/timeseries-integration-timescale)

This plugin exposes an implementation of the TimeSeriesStorage interface.
It stores the data in a pg_timeseries table.
It can be used in OpenNMS to store and retrieve timeseries data.

## Prerequisite
* The pg_timeseries plugin must be installed in the target postgres database.
* For testing purposes you can run: ``docker run -d --name pg-timeseries -p 5432:5432 -e POSTGRES_PASSWORD=postgres quay.io/tembo/timeseries-pg:latest``

## Usage
### enable the Time Series Storage layer
* In opennms deploy root folder: ``echo "org.opennms.timeseries.strategy=integration" >> etc/opennms.properties.d/timescale.properties``
### Compile from source
* compile: ``mvn install``
* copy the timeseries-pgtimeseries-plugin.kar from the ./assembly/kar/target folder ot $OPENNMS_HOME/deploy
###
### activate in Karaf shell:
  * ``ssh -p 8101 admin@localhost``
  * ``feature:install opennms-plugins-pgtimeseries-timescale-plugin``
  * The plugin will automatically create the necessary tables if they don't already exist.

## Links:
* Introduction to the Time Series Storage Layer: https://docs.opennms.com/horizon/latest/operation/operation/timeseries/introduction.html
* pg_timeseries: https://github.com/tembo-io/pg_timeseries




