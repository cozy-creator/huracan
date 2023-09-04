use influxdb::{InfluxDbWriteable, Timestamp};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn, debug};
use crate::conf::get_influx_singleton;

// Please see tag vs field docs before adding additional structs:
// https://docs.influxdata.com/influxdb/v2.7/write-data/best-practices/schema-design/#use-tags-and-fields

// Created new object in MongoDB.
#[derive(InfluxDbWriteable)]
pub struct InsertObject {
    pub(crate) time: Timestamp,
    pub(crate) count: i32,
}

// Modified or deleted an object in MongoDB.
#[derive(InfluxDbWriteable)]
pub struct ModifiedObject {
    pub(crate) time: Timestamp,
    pub(crate) count: i32,
}

// Attempted to update an object to MongoDB, but it wasn't found.
#[derive(InfluxDbWriteable)]
pub struct MissingObject {
    pub(crate) time: Timestamp,
    pub(crate) count: i32,
}

// Error writing to MongoDB.
#[derive(InfluxDbWriteable)]
pub struct MongoWriteError {
    pub(crate) time: Timestamp,
}

pub async fn write_metric_mongo_write_error() {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = MongoWriteError {
        time,
    }.into_query("mongo_write_error",);
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

// Finished processing a new checkpoint.
#[derive(InfluxDbWriteable)]
pub struct CreateCheckpoint {
    pub(crate) time: Timestamp,
    pub(crate) checkpoint_id: String,
}

pub async fn write_metric_create_checkpoint(checkpoint_id: String) {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = CreateCheckpoint {
        time,
        checkpoint_id,
    }.into_query("create_checkpoint",);
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

// Hit an error writing checkpoint data.
#[derive(InfluxDbWriteable)]
pub struct CheckpointError {
    pub(crate) time: Timestamp,
    pub(crate) checkpoint_id: String,
}

pub async fn write_metric_checkpoint_error(checkpoint_id: String) {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = CheckpointError {
        time,
        checkpoint_id: checkpoint_id.to_string(),
    }.into_query("checkpoint_error",);
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

// Hit some kind of error during ingest.
#[derive(InfluxDbWriteable)]
pub struct IngestError {
    pub(crate) time: Timestamp,
    pub(crate) object_id: String,
    // This could be an enum, but not needed at the moment.
    #[influxdb(tag)] pub(crate) error_type: String,
}

pub async fn write_metric_ingest_error(object_id: &str, error_type: &str) {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = IngestError {
        time,
        object_id: object_id.to_string(),
        error_type: error_type.to_string(),
    }.into_query("ingest_error");
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

// Hit an RPC error.
// If this happens frequently, there may be an issue with the RPC provider.
#[derive(InfluxDbWriteable)]
pub struct RPCError {
    pub(crate) time: Timestamp,
    #[influxdb(tag)] pub(crate) rpc_method: String,
}

pub async fn write_metric_rpc_error(rpc_method: &str) {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = RPCError {
        time,
        rpc_method: rpc_method.to_string(),
    }.into_query("rpc_error");
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

// Sent an RPC request.
#[derive(InfluxDbWriteable)]
pub struct RPCRequest {
    pub(crate) time: Timestamp,
    #[influxdb(tag)] pub(crate) rpc_method: String,
}


pub async fn write_metric_rpc_request(rpc_method: &str) {
    let influx_client = get_influx_singleton();
    let time = get_influx_timestamp_as_milliseconds().await;
    let influx_item = RPCRequest {
        time,
        rpc_method: rpc_method.to_string(),
    }.into_query("rpc_request");
    let write_result = influx_client.query(influx_item).await;
    match write_result {
        Ok(string) => debug!(string),
        Err(error) => warn!("Could not write to influx: {}", error),
    }
}

pub(crate) async fn get_influx_timestamp_as_milliseconds() -> Timestamp {
	let start = SystemTime::now();
	let since_the_epoch = start
		.duration_since(UNIX_EPOCH)
		.expect("Time went backwards")
		.as_millis();
	return Timestamp::Milliseconds(since_the_epoch);
}

