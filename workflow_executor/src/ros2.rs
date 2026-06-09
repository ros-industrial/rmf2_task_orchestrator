use std::sync::mpsc;
use tokio::sync::oneshot;

type Poller = Box<dyn FnMut() -> Option<bool> + Send>;
type WorkFn = Box<dyn FnOnce(&mut r2r::Node) -> Poller + Send>;

pub struct Ros2Session {
    tx: mpsc::Sender<(WorkFn, oneshot::Sender<bool>)>,
}

impl Ros2Session {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<(WorkFn, oneshot::Sender<bool>)>();

        std::thread::spawn(move || {
            let ctx = r2r::Context::create().unwrap();
            let mut node = r2r::Node::create(ctx, "crossflow", "").unwrap();

            let mut pollers: Vec<(Poller, Option<oneshot::Sender<bool>>)> = vec![];

            loop {
                // Register new work
                while let Ok((work_fn, resp_tx)) = rx.try_recv() {
                    let poller = work_fn(&mut node);
                    pollers.push((poller, Some(resp_tx)));
                }

                node.spin_once(std::time::Duration::from_millis(10));

                // Poll for responses
                pollers.retain_mut(|(poller, tx_opt)| {
                    if let Some(result) = poller() {
                        if let Some(tx) = tx_opt.take() {
                            let _ = tx.send(result);
                        }
                        return false;
                    }
                    true
                });
            }
        });

        Self { tx }
    }

    /// Execute a generic pub/sub operation
    /// The closure receives the node, sets up pub/sub, and returns a poller
    /// that checks for responses
    pub async fn execute<F>(&self, work: F) -> Result<bool, String>
    where
        F: FnOnce(&mut r2r::Node) -> Poller + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((Box::new(work), tx))
            .map_err(|_| "send failed")?;
        rx.await.map_err(|_| "recv failed".into())
    }
}
