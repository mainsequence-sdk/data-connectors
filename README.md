# Data Connector for Main Sequence Ecosystems

This project enables the integration of external and local data sources into the Main Sequence Ecosystem. Users can pull data from various APIs or their local systems and load it into the Main Sequence database. This data can then be utilized in downstream strategies and workflows.

## Features

* **Flexible Data Ingestion:** Connect to diverse data sources, including external APIs (e.g., Polygon, Alpaca) and local files.
* **Main Sequence Database Integration:** Seamlessly load acquired data into the Main Sequence database for unified access and management.
* **Automated Data Updates:** Project configurations automatically generate jobs for data updates, which can be further customized.
* **Data Utilization:** Easily access the integrated data as `APITimeseries` within Main Sequence strategies and workflows.
* **Extensible:** Easily adaptable to integrate new and custom data sources by creating new TimeSeries.

## Getting Started

### Integrate into your Main Sequence Organization

You can either adapt the data-connectors project within your Main Sequence organization or fork this repository and adapt it to your needs. If you fork the repo, you'll need to create a new project in your Main Sequence organization and add the forked repository's credentials.

### Configuration

For external data sources like Polygon or Alpaca, ensure you set the corresponding API keys within the project's settings. For local development, set the API keys in an `.env` file in the repository root.

Keep or adapt the project configuration and `TimeSeries` to define your data sources and how data should be ingested. All jobs defined in the project configuration are automatically created in the Main Sequence platform.

### Accessing Data in Main Sequence

After an update has been completed (typically within the `_run_post_update_routines` method), ensure a `MarketsTimeSeriesDetails` is registered for the TimeSerie. In other projects within the Main Sequence Ecosystem, you can then easily access this data as an `APITimeseries` using the `unique_identifier` (or name) of the registered `MarketsTimeSeriesDetails`.

---
