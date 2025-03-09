mod auth;
mod consumer;
mod types;

use auth::AuthTokenProvider;
use consumer::*;
use types::*;

use napi::{Error, JsFunction, Result, Status};
use napi_derive::napi;

// We export a new struct to Node.js as our consumer wrapper.
#[napi]
pub struct PulsarConsumerWrapper {
  inner: Option<PulsarConsumer>,
  config: PulsarConsumerConfig,
  auth: AuthTokenProvider,
}

#[napi]
impl PulsarConsumerWrapper {
  /// Async factory method to create a new PulsarConsumerWrapper.
  /// Called from Node.js as:
  /// ```js
  /// const client = await PulsarConsumerWrapper.new(brokerUrl, topic, subscriptionName, consumerName, consumerId);
  /// ```
  #[napi(factory)]
  pub fn new(
    broker_url: String,
    topic: String,
    subscription_name: String,
    consumer_name: String,
    consumer_id: String,
    auth_provider: JsFunction,
  ) -> Result<Self> {
    println!(
            "[RUST] Creating new Pulsar consumer with broker_url: {}, topic: {}, subscription_name: {}, consumer_name: {}, consumer_id: {}",
            broker_url, topic, subscription_name, consumer_name, consumer_id
        );

    let config = PulsarConsumerConfig {
      broker_url,
      topic,
      subscription_name,
      consumer_name,
      consumer_id,
    };

    let auth = AuthTokenProvider::new(auth_provider);

    Ok(PulsarConsumerWrapper {
      inner: None,
      config,
      auth,
    })
  }

  #[napi]
  pub async unsafe fn start(&mut self) -> Result<()> {
    let consumer = PulsarConsumer::new(self.config.clone(), self.auth.clone())
      .await
      .map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Error creating Pulsar consumer: {:?}", e),
        )
      })?;
    self.inner = Some(consumer);
    Ok(())
  }

  /// Retrieves the next message from Pulsar.
  /// On success, returns a PulsarMessage.
  #[napi]
  pub async unsafe fn next(&mut self) -> Result<PulsarMessage> {
    match self.inner {
      Some(ref mut consumer) => consumer.next().await.map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Error getting next message: {:?}", e),
        )
      }),
      None => Err(Error::new(
        Status::GenericFailure,
        "Consumer not initialized".to_owned(),
      )),
    }
  }

  /// Acknowledges a message given its ID.
  #[napi]
  pub async unsafe fn ack(&mut self, message_id: PulsarMessageId) -> Result<()> {
    match self.inner {
      Some(ref mut consumer) => consumer.ack(message_id).await.map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Error acknowledging message: {:?}", e),
        )
      }),
      None => Err(Error::new(
        Status::GenericFailure,
        "Consumer not initialized".to_owned(),
      )),
    }
  }

  /// Negative-acknowledges a message given its ID.
  #[napi]
  pub async unsafe fn nack(&mut self, message_id: PulsarMessageId) -> Result<()> {
    match self.inner {
      Some(ref mut consumer) => consumer.nack(message_id).await.map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Error negative acknowledging message: {:?}", e),
        )
      }),
      None => Err(Error::new(
        Status::GenericFailure,
        "Consumer not initialized".to_owned(),
      )),
    }
  }

  /// Closes the consumer.
  #[napi]
  pub async unsafe fn close(&mut self) -> Result<()> {
    match self.inner.take() {
      Some(mut consumer) => consumer.close().await.map_err(|e| {
        Error::new(
          Status::GenericFailure,
          format!("Error closing consumer: {:?}", e),
        )
      }),
      None => Err(Error::new(
        Status::GenericFailure,
        "Consumer not initialized".to_owned(),
      )),
    }
  }
}
