use crate::_prelude::*;

use anyhow::Result;

use pulsar::{
    compression::*, consumer::InitialPosition, producer, Authentication, Consumer, ConsumerOptions,
    DeserializeMessage, Producer, Pulsar, SubType, TokioExecutor,
};

pub struct PulsarProducerOptions {
    pub uri: String,
    pub topic: String,
    pub producer: String,
    pub token: Option<String>,
    pub batch_size: u32,
}

pub struct PulsarConsumerOptions {
    pub uri: String,
    pub topic: String,
    pub consumer: String,
    pub subscription: String,
    pub token: Option<String>,
}

async fn create_pulsar(uri: &str, token: &Option<String>) -> Result<Pulsar<TokioExecutor>> {
    let mut builder = Pulsar::builder(uri, TokioExecutor);

    if let Some(token) = &token {
        let auth = Authentication {
            name: "token".to_string(),
            data: token.clone().into_bytes(),
        };
        builder = builder.with_auth(auth);
    }

    Ok(builder.build().await?)
}

pub async fn create_pulsar_producer(
    options: &PulsarProducerOptions,
) -> Result<Producer<TokioExecutor>> {
    let pulsar = create_pulsar(&options.uri, &options.token).await?;

    let producer = pulsar
        .producer()
        .with_topic(&options.topic)
        .with_name(&options.producer)
        .with_options(producer::ProducerOptions {
            batch_size: Some(options.batch_size),
            compression: Some(Compression::Snappy(CompressionSnappy::default())),
            ..Default::default()
        })
        .build()
        .await
        .context("cannot create apache pulsar producer")?;

    producer
        .check_connection()
        .await
        .context("cannot check apache pulsar connection")?;

    Ok(producer)
}

pub async fn create_pulsar_consumer<T: DeserializeMessage>(
    options: &PulsarConsumerOptions,
) -> Result<Consumer<T, TokioExecutor>> {
    let pulsar = create_pulsar(&options.uri, &options.token).await?;

    let mut consumer = pulsar
        .consumer()
        .with_topic(&options.topic)
        .with_consumer_name(&options.consumer)
        .with_subscription_type(SubType::Shared)
        .with_subscription(&options.subscription)
        .with_options(ConsumerOptions {
            initial_position: InitialPosition::Earliest,
            ..ConsumerOptions::default()
        })
        .build()
        .await
        .context("cannot create apache pulsar producer")?;

    consumer
        .check_connection()
        .await
        .context("cannot check apache pulsar connection")?;

    Ok(consumer)
}
