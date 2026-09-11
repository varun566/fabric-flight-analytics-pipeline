# Power BI Report Specification

Create a Direct Lake semantic model named `Flight Operations Analytics` from the two Gold Lakehouse tables:

- `gold_daily_route_performance`
- `gold_hourly_volume`

Apply the measures in `flight_ops_measures.dax`. Format the two percentage measures as Percentage with one decimal place and the average delay as a decimal number with one decimal place.

## Report Page: Flight Operations Overview

| Visual | Fields and configuration |
| --- | --- |
| On-time trend | Line chart: axis `flight_date`, value `On-Time Performance %`; add `route` as a report-page slicer. |
| Route comparison | Clustered bar chart: axis `route`, value `Average Delay (min)`; sort descending and apply a Top N 15 filter. |
| Anomaly alert table | Table: `flight_date`, `route`, `avg_delay_min`, `delay_z_score`, `cancellation_rate`; visual filter `delay_anomaly_flag` is True. |
| Passenger throughput | Column chart: axis `scheduled_dep_hour`, value `Passenger Throughput`; optionally slice by `flight_date` and `origin`. |
| KPI cards | `On-Time Performance %`, `Average Delay (min)`, `Cancellation Rate %`, and `Anomalous Route Days`. |

No relationship is required for the specified visuals because each visual uses one Gold fact table. For a combined date-sliced model, add a calendar table and relate it to `flight_date` in both tables.

