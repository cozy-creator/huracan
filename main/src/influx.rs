use influxdb::{Client, Query, Timestamp, ReadQuery};
use influxdb::InfluxDbWriteable;
use chrono::{DateTime, Utc};


// We write a data point whenever we hit an error during ingest.
#[derive(InfluxDbWriteable)]
pub struct CreateObject {
    time: DateTime<Utc>,
    object_id: String,
    #[influxdb(tag)] error_type: String,
}

// We write a data point whenever we hit an error during ingest.
#[derive(InfluxDbWriteable)]
pub struct IngestError {
    pub(crate) time: Timestamp,
    pub(crate) object_id: String,
    #[influxdb(tag)] pub(crate) error_type: String,
}

// We write a data point whenever we skip an object during ingest.
#[derive(InfluxDbWriteable)]
pub struct IngestSkippedObject {
    time: DateTime<Utc>,
    object_id: String,
    #[influxdb(tag)] object_type: String,
}
