use crate::{config::RedisConfig, operator::ExchangeData};

use super::{ConnectorSourceStrategy, GroupStreamElement};

pub struct RedisSourceConnector<T: ExchangeData> {
    client: redis::Client,
    connection: Option<redis::Connection>,
    key: String,
    final_key: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: ExchangeData> Clone for RedisSourceConnector<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            connection: None,
            key: self.key.clone(),
            final_key: self.final_key.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> std::fmt::Debug for RedisSourceConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnector")
            .field("client", &self.client)
            .field("key", &self.key)
            .field("final_key", &self.final_key)
            .finish()
    }
}

impl<T: ExchangeData> RedisSourceConnector<T> {
    pub fn new(client: redis::Client, key: &str) -> Self {
        RedisSourceConnector {
            client,
            key: key.to_string(),
            final_key: None,
            connection: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ExchangeData> ConnectorSourceStrategy<T> for RedisSourceConnector<T> {
    fn replication(&self) -> crate::Replication {
        crate::Replication::Unlimited
    }

    fn setup(&mut self, metadata: &mut crate::ExecutionMetadata) {
        self.final_key = Some(format!("{}-{}", self.key, metadata.global_id));
        self.connection = self.client.get_connection().ok();
    }

    fn next(&mut self) -> GroupStreamElement<T> {
        let connection = self.connection.as_mut().unwrap();
        let value = redis::cmd("BLPOP") // Blocking Left POP
            .arg(self.final_key.as_ref().unwrap())
            .arg(0)
            .query(connection)
            .expect("Cannot pop from redis");
        let (_, value): (String, String) = value;
        serde_json::from_str(value.as_str()).expect("Cannot deserialize value")
    }

    fn technology(&self) -> String {
        "Redis".to_string()
    }
}

impl<T: ExchangeData> From<&RedisConfig> for RedisSourceConnector<T> {
    fn from(value: &RedisConfig) -> Self {
        let client = redis::Client::open(value.urls.first().unwrap().to_string()).unwrap();
        RedisSourceConnector::new(client, value.key.as_str())
    }
}
