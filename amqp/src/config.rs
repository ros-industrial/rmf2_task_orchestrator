#[derive(serde::Deserialize, Clone)]
pub struct AmqpSettings {
    pub host: String,
    pub port: u16,
    pub consumer: ConsumerSettings,
}

#[derive(serde::Deserialize, Clone)]
pub struct ConsumerSettings {
    pub exchange: String,
    pub queue: String,
}

impl AmqpSettings {
    pub fn to_url(&self) -> String {
        format!("amqp://{}:{}", self.host, self.port)
    }
}
