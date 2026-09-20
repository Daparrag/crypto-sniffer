use thiserror::Error;
use {
    async_trait::async_trait,
    clap::ValueEnum,
    yellowstone_grpc_client::{
        Backoff, DEFAULT_SLOT_RETENTION, GeyserGrpcClient, GeyserStream, ReconnectConfig,
        ReconnectionPolicy,
    },
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReconnectingPolicy {
    Recover,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Down,
    Up,
    Reconnecting,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("client error: {0}")]
    ClientError(String),
    #[error("configuration error: {0}")]
    ConfigurationError(String),
    #[error("connection lost: {0}")]
    ConnectionLost(String),
}

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    endpoint: String,
    x_token: Option<String>,
    policy: ReconnectingPolicy,
}

impl ConnectorConfig {
    pub fn new(endpoint: impl Into<String>) -> Result<Self, ConnectionError> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(ConnectionError::ConfigurationError(
                "endpoint cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            endpoint: endpoint.into(),
            x_token: None,
            policy: ReconnectingPolicy::Recover,
        })
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.x_token = Some(token.into());
        self
    }

    pub fn with_policy(mut self, policy: ReconnectingPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_config(mut self, config: ConnectorConfig) -> Self {
        self.endpoint = config.endpoint;
        self.x_token = config.x_token;
        self.policy = config.policy;
        self
    }
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:10000".to_string(),
            x_token: None,
            policy: ReconnectingPolicy::Recover,
        }
    }
}

#[async_trait]
pub trait Connector: Send + Sync {
    async fn connect(&mut self) -> Result<(), ConnectionError>;
    async fn disconnect(&mut self) -> Result<(), ConnectionError>;
    async fn subscribe_to(&mut self, filters: Vec<Filter>)
    -> Result<GeyserStream, ConnectionError>;
    fn status(&self) -> ConnectionStatus;
    fn config(&self) -> &ConnectorConfig;
}

#[derive(Debug, Clone)]
pub struct NamedFilter<T> {
    name: String,
    filter: T,
}
pub type GeyserSlots = NamedFilter<SubscribeRequestFilterSlots>;
pub type GeyserAccount = NamedFilter<SubscribeRequestFilterAccounts>;
pub type GeyserTransaction = NamedFilter<SubscribeRequestFilterTransactions>;

impl<T> NamedFilter<T> {
    pub fn new(name: impl Into<String>, filter: T) -> Self {
        Self {
            name: name.into(),
            filter,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T: Default> Default for NamedFilter<T> {
    fn default() -> Self {
        Self {
            name: String::new(),
            filter: T::default(),
        }
    }
}

impl NamedFilter<SubscribeRequestFilterSlots> {
    pub fn slots() -> Self {
        Self::new(
            "slots",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(true),
            },
        )
    }
}

impl NamedFilter<SubscribeRequestFilterAccounts> {
    pub fn accounts() -> Self {
        Self::new("accounts", SubscribeRequestFilterAccounts::default())
    }
}

impl NamedFilter<SubscribeRequestFilterTransactions> {
    pub fn transactions() -> Self {
        Self::new(
            "transactions",
            SubscribeRequestFilterTransactions::default(),
        )
    }
}
#[derive(Debug, Clone)]
pub enum Filter {
    Slots(GeyserSlots),
    Accounts(GeyserAccount),
    Transactions(GeyserTransaction),
}

impl Filter {
    fn add_to_request(&self, request: &mut SubscribeRequest) {
        match self {
            Self::Slots(filter) => {
                request
                    .slots
                    .insert(filter.name.clone(), filter.filter.clone());
            }

            Self::Accounts(filter) => {
                request
                    .accounts
                    .insert(filter.name.clone(), filter.filter.clone());
            }

            Self::Transactions(filter) => {
                request
                    .transactions
                    .insert(filter.name.clone(), filter.filter.clone());
            }
        }
    }
}

pub struct GeyserConnector {
    name: String,
    config: ConnectorConfig,
    status: ConnectionStatus,
    client: Option<GeyserGrpcClient>,
}

impl GeyserConnector {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: ConnectorConfig::default(),
            status: ConnectionStatus::Down,
            client: None,
        }
    }
    pub fn with_config(mut self, config: ConnectorConfig) -> Self {
        self.config = config;
        self
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl Connector for GeyserConnector {
    async fn connect(&mut self) -> Result<(), ConnectionError> {
        let policy = match self.config.policy {
            ReconnectingPolicy::Recover => ReconnectionPolicy::RecoverMissedData {
                slot_retention: DEFAULT_SLOT_RETENTION,
            },

            ReconnectingPolicy::Skip => ReconnectionPolicy::SkipMissedData,
        };

        let reconnect_config = ReconnectConfig {
            backoff: Backoff::default(),
            policy,
        };

        let client = GeyserGrpcClient::build_from_shared(self.config.endpoint.clone())
            .map_err(|error| ConnectionError::ClientError(error.to_string()))?
            .x_token(self.config.x_token.clone())
            .map_err(|error| ConnectionError::ClientError(error.to_string()))?
            .set_reconnect_config(reconnect_config)
            .connect()
            .await
            .map_err(|error| ConnectionError::ClientError(error.to_string()))?;

        self.client = Some(client);
        self.status = ConnectionStatus::Up;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), ConnectionError> {
        self.client = None;
        self.status = ConnectionStatus::Down;
        Ok(())
    }

    async fn subscribe_to(
        &mut self,
        filters: Vec<Filter>,
    ) -> Result<GeyserStream, ConnectionError> {
        let request = build_request(&filters);

        let client = self.client.as_mut().ok_or_else(|| {
            ConnectionError::ConnectionLost("cannot subscribe before connecting".to_string())
        })?;

        client
            .subscribe_once(request)
            .await
            .map_err(|error| ConnectionError::ClientError(error.to_string()))
    }

    fn status(&self) -> ConnectionStatus {
        self.status
    }

    fn config(&self) -> &ConnectorConfig {
        &self.config
    }
}

pub fn build_request(filters: &[Filter]) -> SubscribeRequest {
    let mut request = SubscribeRequest {
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    for filter in filters {
        filter.add_to_request(&mut request);
    }

    request
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builds_slot_request() {
        let request = build_request(&[Filter::Slots(GeyserSlots::slots())]);

        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));

        assert!(request.slots.contains_key("slots"));
        assert!(request.accounts.is_empty());
        assert!(request.transactions.is_empty());

        let slots = request.slots.get("slots").unwrap();

        assert_eq!(slots.filter_by_commitment, Some(true));
        assert_eq!(slots.interslot_updates, Some(true));
    }

    #[test]
    fn builds_all_filter_types() {
        let request = build_request(&[
            Filter::Slots(GeyserSlots::slots()),
            Filter::Accounts(GeyserAccount::accounts()),
            Filter::Transactions(GeyserTransaction::transactions()),
        ]);

        assert!(request.slots.contains_key("slots"));
        assert!(request.accounts.contains_key("accounts"));
        assert!(request.transactions.contains_key("transactions"));
    }

    #[test]
    fn preserves_custom_filter_name() {
        let filter = GeyserSlots::new(
            "custom-slots",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        );

        let request = build_request(&[Filter::Slots(filter)]);

        assert!(request.slots.contains_key("custom-slots"));
        assert!(!request.slots.contains_key("slots"));
    }

    #[test]
    fn duplicate_names_replace_previous_filter() {
        let first = GeyserSlots::new(
            "same-name",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(false),
            },
        );

        let second = GeyserSlots::new(
            "same-name",
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(true),
            },
        );

        let request = build_request(&[Filter::Slots(first), Filter::Slots(second)]);

        assert_eq!(request.slots.len(), 1);

        let slots = request.slots.get("same-name").unwrap();

        assert_eq!(slots.filter_by_commitment, Some(true));
        assert_eq!(slots.interslot_updates, Some(true));
    }
    #[test]
    fn builds_request_with_no_filters() {
        let request = build_request(&[]);

        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));

        assert!(request.slots.is_empty());
        assert!(request.accounts.is_empty());
        assert!(request.transactions.is_empty());
    }
    #[test]
    fn preserves_custom_account_filter() {
        let accounts = GeyserAccount::new(
            "wallets",
            SubscribeRequestFilterAccounts {
                account: vec!["account-address".to_string()],
                ..Default::default()
            },
        );

        let request = build_request(&[Filter::Accounts(accounts)]);

        let filter = request.accounts.get("wallets").unwrap();

        assert_eq!(filter.account, vec!["account-address"]);
    }
    #[test]
    fn preserves_custom_transaction_filter() {
        let transactions = GeyserTransaction::new(
            "transfers",
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                ..Default::default()
            },
        );

        let request = build_request(&[Filter::Transactions(transactions)]);

        let filter = request.transactions.get("transfers").unwrap();

        assert_eq!(filter.vote, Some(false));
        assert_eq!(filter.failed, Some(false));
    }
    #[test]
    fn default_config_is_valid() {
        let config = ConnectorConfig::default();

        assert_eq!(config.endpoint, "http://127.0.0.1:10000");
        assert!(config.x_token.is_none());
    }
    #[tokio::test]
    async fn subscribe_before_connect_returns_connection_error() {
        let config = ConnectorConfig::default();
        let mut connector = GeyserConnector::new("test").with_config(config);

        let result = connector
            .subscribe_to(vec![Filter::Slots(GeyserSlots::slots())])
            .await;

        assert!(matches!(result, Err(ConnectionError::ConnectionLost(_))));
    }
    #[tokio::test]
    async fn disconnect_sets_status_down() {
        let mut connector = GeyserConnector::new("test");

        assert_eq!(connector.status(), ConnectionStatus::Down);

        connector.disconnect().await.unwrap();

        assert_eq!(connector.status(), ConnectionStatus::Down);
    }
}
