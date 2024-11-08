use crate::{
    config::RedisConfig,
    operator::{ExchangeData, StreamElement},
    ExecutionMetadata,
};

use super::ConnectorSinkStrategy;

pub struct RedisSinkConnector<T: ExchangeData> {
    client: redis::Client,
    key: String,
    final_key: Option<String>,
    maybe_connection: Option<redis::Connection>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> Clone for RedisSinkConnector<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            key: self.key.clone(),
            final_key: self.final_key.clone(),
            maybe_connection: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> std::fmt::Debug for RedisSinkConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSink")
            .field("client", &self.client)
            .field("key", &self.key)
            .field("final_key", &self.final_key)
            .finish()
    }
}

impl<T: ExchangeData> RedisSinkConnector<T> {
    pub fn new(client: redis::Client, key: String) -> Self {
        Self {
            client,
            key,
            final_key: None,
            maybe_connection: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> ConnectorSinkStrategy<T> for RedisSinkConnector<T> {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.final_key = Some(format!("{}-{}", self.key, metadata.global_id));
        self.maybe_connection = self.client.get_connection().ok();
    }

    fn append(&mut self, item: &StreamElement<T>) {
        let connection = self
            .maybe_connection
            .as_mut()
            .expect("Cannot get redis connection");
        redis::cmd("RPUSH")
            .arg(self.final_key.as_ref().unwrap())
            .arg(serde_json::to_string(item).unwrap())
            .query::<()>(connection)
            .expect("Cannot push to redis");
    }

    fn technology(&self) -> String {
        "Redis".to_string()
    }
}

impl<T: ExchangeData> From<&RedisConfig> for RedisSinkConnector<T> {
    fn from(value: &RedisConfig) -> Self {
        let url = value.urls.first().unwrap();
        let client = redis::Client::open(url.to_string()).unwrap();
        RedisSinkConnector::new(client, value.key.clone()) // TODO change to redis cluster
    }
}
