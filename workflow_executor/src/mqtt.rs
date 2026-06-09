use dashmap::DashMap;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub type MqttMessage = Vec<u8>;

#[derive(Clone)]
pub struct MqttHandle {
    client: AsyncClient,
    subscriptions: Arc<DashMap<String, Vec<mpsc::Sender<MqttMessage>>>>,
}

impl MqttHandle {
    pub async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<mpsc::Receiver<MqttMessage>, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel(32);
        self.client
            .subscribe(topic, QoS::AtMostOnce)
            .await
            .map_err(|e| format!("Failed to subscribe to {topic} topic: {e}"))?;
        let mut senders = self.subscriptions
            .entry(topic.to_string())
            .or_default();
        // Prune dead senders from previous workflows before adding new one
        senders.retain(|tx| !tx.is_closed());
        senders.push(tx);
        Ok(rx)
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn subscriber_count(&self, topic: &str) -> usize {
        self.subscriptions
            .get(topic)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .publish(topic, QoS::AtMostOnce, false, payload)
            .await
            .map_err(|e| format!("Failed to publish to {topic} topic: {e}"))?;
        Ok(())
    }
}

pub fn mqtt_setup(
    client_id: &str,
    mqtt_host: &str,
    mqtt_port: u16,
) -> Result<MqttHandle, Box<dyn std::error::Error>> {
    let mut mqttoptions = MqttOptions::new(client_id, mqtt_host, mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 4096);
    let subscriptions: Arc<DashMap<String, Vec<mpsc::Sender<MqttMessage>>>> = Arc::new(DashMap::new());
    let subs = subscriptions.clone();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let topic = publish.topic.as_str();
                    if let Some(mut senders) = subs.get_mut(topic) {
                        let payload = publish.payload.to_vec();
                        // Remove closed channels and send to all active subscribers
                        senders.retain(|tx| tx.try_send(payload.clone()).is_ok());
                        if senders.is_empty() {
                            drop(senders);
                            subs.remove(topic);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("MQTT connection error, reconnecting... {e}");
                }
            }
        }
    });
    Ok(MqttHandle {
        client,
        subscriptions,
    })
}
