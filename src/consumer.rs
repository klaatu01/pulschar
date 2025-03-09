use crate::{auth::AuthTokenProvider, types::*};
use futures::StreamExt;
use pulsar::{
  consumer::{data::MessageData, InitialPosition},
  Consumer, Pulsar, TokioExecutor,
};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PulsarConsumerConfig {
  pub broker_url: String,
  pub topic: String,
  pub subscription_name: String,
  pub consumer_name: String,
  pub consumer_id: String,
}

pub struct PulsarConsumer {
  pub config: PulsarConsumerConfig,
  pub inner: Consumer<DataContainer, TokioExecutor>,
}

impl PulsarConsumer {
  pub async fn new(
    config: PulsarConsumerConfig,
    auth: AuthTokenProvider,
  ) -> Result<PulsarConsumer, PulsarError> {
    env_logger::init();

    println!(
            "[RUST] Connecting to Pulsar broker_url: {}, topic: {}, subscription_name: {}, consumer_name: {}, consumer_id: {}",
            config.broker_url, config.topic, config.subscription_name, config.consumer_name, config.consumer_id
        );

    let pulsar_builder = Pulsar::builder(config.broker_url.clone(), TokioExecutor)
      .with_operation_retry_options(pulsar::OperationRetryOptions {
        operation_timeout: Duration::from_secs(30),
        retry_delay: Duration::from_secs(5),
        max_retries: Some(60),
      })
      .with_auth_provider(Box::new(auth))
      .build()
      .await
      .map_err(|e| PulsarError::ConsumerError(format!("Error building pulsar: {:?}", e)))?;

    println!("[RUST] Connected.");

    println!(
      "[RUST] Building consumer with topic: {}, subscription_name: {}, consumer_name: {}",
      config.topic, config.subscription_name, config.consumer_name
    );

    pulsar_builder
      .consumer()
      .with_topic(config.topic.clone())
      .with_consumer_name(config.consumer_name.clone())
      .with_subscription_type(pulsar::SubType::Shared) // What subsription type you need is up to you, but this is a good default as it won't mess with other consumers
      .with_subscription(config.subscription_name.clone())
      .with_options(pulsar::ConsumerOptions {
        initial_position: InitialPosition::Earliest,
        ..Default::default()
      })
      .with_unacked_message_resend_delay(Some(Duration::from_secs(60)))
      .build()
      .await
      .map_err(|e| PulsarError::ConsumerError(format!("Error building consumer: {:?}", e)))
      .map(|inner| PulsarConsumer { config, inner })
  }

  pub async fn next(&mut self) -> Result<PulsarMessage, PulsarError> {
    println!("[RUST] Waiting for next message");
    let message = self.inner.next().await;
    match message {
      Some(msg) => msg
        .map(|m| PulsarMessage {
          data: m.payload.data,
          message_id: m.message_id.into(),
        })
        .map_err(|e| PulsarError::ConsumerError(format!("Error consuming message: {:?}", e))),
      None => Err(PulsarError::ConsumerError("No message".to_string())),
    }
  }

  pub async fn ack(&mut self, message_id: PulsarMessageId) -> Result<(), PulsarError> {
    println!("[RUST] Acknowledging message");
    let message_id: MessageData = message_id.into();
    self
      .inner
      .ack_with_id(&self.config.topic, message_id.id)
      .await
      .map_err(|e| PulsarError::ConsumerError(format!("Error acknowledging message: {:?}", e)))
  }

  pub async fn nack(&mut self, message_id: PulsarMessageId) -> Result<(), PulsarError> {
    println!("[RUST] Negative acknowledging message");
    let message_id: MessageData = message_id.into();
    self
      .inner
      .nack_with_id(&self.config.topic, message_id.id)
      .await
      .map_err(|e| PulsarError::ConsumerError(format!("Error acknowledging message: {:?}", e)))
  }

  pub async fn close(&mut self) -> Result<(), PulsarError> {
    self
      .inner
      .close()
      .await
      .map_err(|e| PulsarError::ConsumerError(format!("Error closing consumer: {:?}", e)))
  }
}
