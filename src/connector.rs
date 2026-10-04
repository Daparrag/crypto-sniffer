use futures::stream::Stream;
use log::{debug, info};
use thiserror::Error;
use {
    async_trait::async_trait,
    clap::ValueEnum,
    yellowstone_grpc_client::{
        Backoff, ClientTlsConfig, DEFAULT_SLOT_RETENTION, GeyserGrpcClient, GeyserStream,
        ReconnectConfig, ReconnectionPolicy,
    },
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
    yellowstone_grpc_proto::tonic::Status,
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
            endpoint,
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

impl PartialEq for ConnectorConfig {
    fn eq(&self, other: &Self) -> bool {
        (self.endpoint == other.endpoint)
            && (self.x_token == other.x_token)
            && (self.policy == other.policy)
    }
}

#[async_trait]
pub trait Connector: Send + Sync {
    type StreamItem;
    type StreamError;
    type StreamType: Stream<Item = Result<Self::StreamItem, Self::StreamError>> + Send + Unpin;

    async fn connect(&mut self) -> Result<(), ConnectionError>;
    async fn disconnect(&mut self) -> Result<(), ConnectionError>;
    async fn subscribe_to(
        &mut self,
        filters: Vec<Filter>,
    ) -> Result<Self::StreamType, ConnectionError>;
    #[allow(dead_code)]
    fn status(&self) -> ConnectionStatus;
    #[allow(dead_code)]
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
                request.slots.insert(filter.name.clone(), filter.filter);
                debug!("Using Filter {}", filter.name());
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

    pub fn build_connector(
        endpoint: String,
        policy: ReconnectingPolicy,
        x_token: Option<String>,
        name: &str,
    ) -> Result<GeyserConnector, ConnectionError> {
        let mut config = ConnectorConfig::new(endpoint.clone())?.with_policy(policy);
        if let Some(token) = &x_token {
            config = config.with_token(token.clone());
        }
        Ok(GeyserConnector::new(name).with_config(config))
    }

    pub fn build_filters(
        &self,
        enable_slots: bool,
        enable_accounts: bool,
        enable_transactions: bool,
    ) -> Vec<Filter> {
        let mut filters = Vec::new();
        if enable_slots {
            filters.push(Filter::Slots(GeyserSlots::slots()));
        }

        if enable_accounts {
            filters.push(Filter::Accounts(GeyserAccount::accounts()));
        }

        if enable_transactions {
            filters.push(Filter::Transactions(GeyserTransaction::transactions()));
        }

        filters
    }
}

#[async_trait]
impl Connector for GeyserConnector {
    type StreamItem = SubscribeUpdate;
    type StreamError = Status;
    type StreamType = GeyserStream;

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
        info!(
            "Connecting to enpoint={} with policy={:?} token configure={}",
            self.config.endpoint,
            self.config.policy,
            self.config.x_token.is_some()
        );

        let client = GeyserGrpcClient::build_from_shared(self.config.endpoint.clone())
            .map_err(|error| ConnectionError::ClientError(error.to_string()))?
            .x_token(self.config.x_token.clone())
            .map_err(|error| ConnectionError::ClientError(error.to_string()))?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|error| ConnectionError::ClientError(format!("{error:?}")))?
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
    ) -> Result<Self::StreamType, ConnectionError> {
        let request = build_request(&filters);

        let client = self.client.as_mut().ok_or_else(|| {
            ConnectionError::ConnectionLost("cannot subscribe before connecting".to_string())
        })?;

        client
            .subscribe_once(request)
            .await
            .map_err(|error| ConnectionError::ClientError(error.to_string()))
    }
    #[allow(dead_code)]
    fn status(&self) -> ConnectionStatus {
        self.status
    }
    #[allow(dead_code)]
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
    fn filter_name_is_preserved() {
        assert_eq!(
            GeyserSlots::new("slots", SubscribeRequestFilterSlots::default()).name(),
            "slots"
        );
        assert_eq!(
            GeyserAccount::new("accounts", SubscribeRequestFilterAccounts::default()).name(),
            "accounts"
        );
        assert_eq!(
            GeyserTransaction::new(
                "transactions",
                SubscribeRequestFilterTransactions::default()
            )
            .name(),
            "transactions"
        );
        assert_eq!(GeyserSlots::default().name(), "");
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

    #[test]
    fn config_valid_after_setup() {
        let config = ConnectorConfig::default();
        let connector = GeyserConnector::new("test").with_config(config);

        assert_eq!(connector.config().endpoint, "http://127.0.0.1:10000");
        assert!(connector.config.x_token.is_none());
        assert_eq!(connector.config.policy, ReconnectingPolicy::Recover);
    }
    #[test]
    fn build_connector_from_parameters() {
        let endpoint = "http://127.0.0.1:10000".to_string();
        let policy = ReconnectingPolicy::Skip;
        let x_token = Some("123456789".to_string());
        let name = "solana";

        let connector = GeyserConnector::build_connector(endpoint, policy, x_token, name).unwrap();
        assert_eq!(connector.config().endpoint, "http://127.0.0.1:10000");
        assert_eq!(connector.config().policy, ReconnectingPolicy::Skip);
        assert_eq!(connector.config().x_token, Some("123456789".to_string()));
        assert_eq!(connector.name(), name);
        assert_eq!(connector.status(), ConnectionStatus::Down);
    }

    #[test]
    fn geyser_connector_exposes_name() {
        let connector = GeyserConnector::new("yellowstone");
        assert_eq!(connector.name(), "yellowstone");
    }
    #[test]
    fn geyser_filters_from_connector() {
        let connector = GeyserConnector::new("yellowstone");
        let filters = connector.build_filters(true, true, false);
        assert!(!filters.is_empty());
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
    #[tokio::test]
    async fn valid_name_after_assigment() {
        let connector_1 = GeyserConnector::new("test");
        let connector_2 = GeyserConnector::new("test2");
        assert_eq!("test", connector_1.name());
        assert_eq!("test2", connector_2.name());
    }
}
