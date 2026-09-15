use {
    clap::{Parser, ValueEnum},
    yellowstone_grpc_client::{
        Backoff, DEFAULT_SLOT_RETENTION, GeyserGrpcClient, ReconnectConfig, ReconnectionPolicy,
    },
};
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReconectingPolicy {
    Recover,
    Skip,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ConnectionStatus {
    Down,
    Up,
    Reconecting,
}

#[derive(Debug)]
enum ConnectionError {
    ClientError(String),
    ConfigurationError(String),
    ConnectionLost(String),
}

impl From<GeyserGrpcBuilderError> for ConnectionError {
    fn from(error: GeyserGrpcBuilderError) -> Self {
        Self::ClientError(error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    endpoint: String,
    x_token: Option<String>,
    policy: ReconnectingPolicy,
}

impl ConnectorConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            x_token: None,
            policy: ReconnectingPolicy::Recover,
        }
    }

    pub fn with_token(mut self, token: String) -> Self {
        self.x_token = Some(token);
        self
    }

    pub fn with_policy(mut self, policy: ReconnectingPolicy) -> Self {
        self.policy = policy;
        self
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {}

#[async_trait]
pub trait Connector: Send + Sync {
    async fn connect(&mut self) -> Result<(), ConnectionError>;
    async fn disconnect(&mut self) -> Result<(), ConnectionError>;
    async fn subscribe(&mut self) -> Result<(), ConnectionError>;
    fn status(&self) -> ConnectionStatus;
    fn config(&self) -> &ConnectorConfig;
}

#[derive(Debug)]
pub struct GeyserConnector {
    config: ConnectorConfig,
    status: ConnectionStatus,
}

impl Connector for GeyserConnector {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        let reconnect_config = ReconnectConfig {
            backoff: Backoff::default(),
            policy: match self.config.policy {
                ReconectingPolicy::Recover => ReconnectionPolicy::RecoverMissedData {
                    slot_retention: DEFAULT_SLOT_RETENTION,
                },
                ReconectingPolicy::Skip => ReconectingPolicy::SkipMissedData,
            },
        };
        GeyserGrpcClient::build_from_shared(self.config.endpoint)?
            .x_token(self.config.x_token)?
            .set_reconnect_config(reconnect_config)
            .connect()
            .await?;
        self.status = ConnectionStatus::Up;
        Ok(())
    }
}
