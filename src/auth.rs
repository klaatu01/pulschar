use async_trait::async_trait;
use napi::{threadsafe_function::ThreadsafeFunction, JsFunction};
use pulsar::{authentication::Authentication, error::AuthenticationError};

use futures::channel::oneshot;
use napi::{bindgen_prelude::Promise, threadsafe_function::ThreadsafeFunctionCallMode};

#[derive(Clone)]
pub struct AuthTokenProvider {
  ts_fn: ThreadsafeFunction<()>,
}

impl AuthTokenProvider {
  pub fn new(js_function: JsFunction) -> AuthTokenProvider {
    let ts_fn = js_function
      .create_threadsafe_function(0, |_| Ok(vec![()]))
      .unwrap();
    AuthTokenProvider { ts_fn }
  }

  pub async fn call_js_auth_fn(&self) -> String {
    let (tx, rx) = oneshot::channel::<Promise<String>>();

    println!("[RUST] Calling JS auth function");

    self.ts_fn.call_with_return_value(
      Ok(()),
      ThreadsafeFunctionCallMode::Blocking,
      move |value: Promise<String>| {
        let _ = tx.send(value);
        Ok(())
      },
    );

    println!("[RUST] Waiting for JS auth function to return");

    let promise: Promise<String> = rx.await.unwrap();

    println!("[RUST] JS auth function returned");

    let value = promise.await.unwrap();

    println!("[RUST] JS auth function returned value: {}", value);

    value
  }
}

#[async_trait]
impl Authentication for AuthTokenProvider {
  fn auth_method_name(&self) -> String {
    "token".to_string()
  }

  async fn initialize(&mut self) -> Result<(), AuthenticationError> {
    Ok(())
  }

  async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
    let token = self.call_js_auth_fn().await;
    Ok(token.into_bytes())
  }
}
