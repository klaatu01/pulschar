use std::error::Error;

use napi_derive::napi;
use pulsar::{
    consumer::data::MessageData, proto::MessageIdData, DeserializeMessage, Payload,
    SerializeMessage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataContainer {
    pub data: Value,
}

impl DeserializeMessage for DataContainer {
    type Output = Result<Self, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        let data = serde_json::from_slice(&payload.data);
        match data {
            Ok(d) => Ok(Self { data: d }),
            Err(e) => Err(e),
        }
    }
}

impl SerializeMessage for DataContainer {
    fn serialize_message(input: Self) -> Result<pulsar::producer::Message, pulsar::Error> {
        Ok(pulsar::producer::Message {
            payload: serde_json::to_vec(&input.data).unwrap(),
            ..Default::default()
        })
    }
}

impl From<Value> for DataContainer {
    fn from(value: Value) -> Self {
        Self { data: value }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum PulsarError {
    ConsumerError(String),
}

impl Error for PulsarError {}

impl std::fmt::Display for PulsarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PulsarError::ConsumerError(e) => write!(f, "Consumer error: {}", e),
        }
    }
}

#[napi(object)]
pub struct PulsarMessage {
    pub data: Vec<u8>,
    pub message_id: PulsarMessageId,
}

#[derive(Clone, PartialEq, Eq, Hash)]
#[napi(object)]
pub struct PulsarMessageId {
    pub id: PulsarMessageIdData,
    pub batch_size: Option<i32>,
}

impl From<PulsarMessageId> for MessageData {
    fn from(val: PulsarMessageId) -> Self {
        MessageData {
            id: val.id.into(),
            batch_size: val.batch_size,
        }
    }
}

impl From<MessageData> for PulsarMessageId {
    fn from(val: MessageData) -> Self {
        PulsarMessageId {
            id: val.id.into(),
            batch_size: val.batch_size,
        }
    }
}

#[derive(Clone, PartialEq, Hash, Eq)]
#[napi(object)]
pub struct PulsarMessageIdData {
    pub ledger_id: String,
    pub entry_id: String,
    pub partition: Option<i32>,
    pub batch_index: Option<i32>,
    pub ack_set: Vec<i64>,
    pub batch_size: Option<i32>,
}

impl From<PulsarMessageIdData> for MessageIdData {
    fn from(val: PulsarMessageIdData) -> Self {
        MessageIdData {
            ledger_id: val.ledger_id.parse().unwrap_or(0),
            entry_id: val.entry_id.parse().unwrap_or(0),
            partition: val.partition,
            batch_index: val.batch_index,
            ack_set: val.ack_set,
            batch_size: val.batch_size,
            first_chunk_message_id: None,
        }
    }
}

impl From<MessageIdData> for PulsarMessageIdData {
    fn from(val: MessageIdData) -> Self {
        PulsarMessageIdData {
            ledger_id: val.ledger_id.to_string(),
            entry_id: val.entry_id.to_string(),
            partition: val.partition,
            batch_index: val.batch_index,
            ack_set: val.ack_set,
            batch_size: val.batch_size,
        }
    }
}
