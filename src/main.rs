mod connector;
mod timer;

use crate::connector::{
    ConnectionError, Connector, ConnectorConfig, Filter, GeyserAccount, GeyserConnector,
    GeyserSlots, GeyserTransaction, ReconnectingPolicy,
};
use crate::timer::{
    EventEnvelope, GeyserEvent, GeyserReader, GeyserReaderError, Reader, ReceiveTimer,
    ReceivedEvent,
};
use clap::Parser;
use log::info;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
    subscribe_update::UpdateOneof,
};

#[derive(Debug, Parser)]
#[command(author, version, about = "Yellowstone gRPC streaming client")]
struct Args {
    /// Yellowstone gRPC endpoint
    #[arg(short, long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    /// Yellowstone authentication token
    #[arg(long)]
    x_token: Option<String>,

    /// Reconnection behavior
    #[arg(
        long,
        value_enum,
        default_value_t = ReconnectingPolicy::Recover
    )]
    policy: ReconnectingPolicy,

    /// Subscribe to slot updates
    #[arg(long)]
    slots: bool,

    /// Subscribe to account updates
    #[arg(long)]
    accounts: bool,

    /// Subscribe to transaction updates
    #[arg(long)]
    transactions: bool,
}

fn handle_slot(envelope: &EventEnvelope<GeyserEvent>, slot: &SubscribeUpdateSlot) {
    info!(
        "slot={} source={:?} epoch={} seq={} recv_ns={}",
        slot.slot,
        envelope.source(),
        envelope.connection_epoch(),
        envelope.recv_seq(),
        envelope.received_mono_ns(),
    );
}

fn handle_account(envelope: &EventEnvelope<GeyserEvent>, account: &SubscribeUpdateAccount) {
    if envelope.recv_seq().is_multiple_of(1000) {
        info!(
            "account_update slot={} source={:?} epoch={} seq={} recv_ns={}",
            account.slot,
            envelope.source(),
            envelope.connection_epoch(),
            envelope.recv_seq(),
            envelope.received_mono_ns(),
        );
    }
}

fn handle_transaction(
    envelope: &EventEnvelope<GeyserEvent>,
    transaction: &SubscribeUpdateTransaction,
) {
    if envelope.recv_seq().is_multiple_of(1000) {
        info!(
            "transaction slot={} source={:?} epoch={} seq={} recv_ns={}",
            transaction.slot,
            envelope.source(),
            envelope.connection_epoch(),
            envelope.recv_seq(),
            envelope.received_mono_ns(),
        );
    }
}

fn handle_event(event: &EventEnvelope<GeyserEvent>) {
    match event.event() {
        GeyserEvent::Slot(slot) => {
            handle_slot(event, slot);
        }

        GeyserEvent::AccountUpdate(account) => {
            handle_account(event, account);
        }

        GeyserEvent::Transactions(tx) => {
            handle_transaction(event, tx);
        }
    }
}

fn normalize_geyser(
    received: ReceivedEvent<SubscribeUpdate>,
) -> Result<EventEnvelope<GeyserEvent>, GeyserReaderError> {
    let (source, connection_epoch, recv_seq, received_mono_ns, payload) = received.into_parts();

    let update = payload
        .update_oneof
        .ok_or(GeyserReaderError::InvalidEvent)?;

    let event = match update {
        UpdateOneof::Slot(slot) => GeyserEvent::Slot(slot),

        UpdateOneof::Account(account) => GeyserEvent::AccountUpdate(account),

        UpdateOneof::Transaction(tx) => GeyserEvent::Transactions(Box::new(tx)),

        _ => {
            return Err(GeyserReaderError::InvalidEvent);
        }
    };

    Ok(EventEnvelope::new(
        source,
        connection_epoch,
        recv_seq,
        received_mono_ns,
        event,
    ))
}

// entry  function for building a geyser connector
fn build_geyser_connector(args: &Args, name: &str) -> Result<GeyserConnector, ConnectionError> {
    let mut config = ConnectorConfig::new(args.endpoint.clone())?.with_policy(args.policy);

    if let Some(token) = &args.x_token {
        config = config.with_token(token.clone());
    }

    Ok(GeyserConnector::new(name).with_config(config))
}

// entry fuction for buliding geyser fiters
fn build_geyser_filters(args: &Args) -> Vec<Filter> {
    let mut filters = Vec::new();

    if args.slots {
        filters.push(Filter::Slots(GeyserSlots::slots()));
    }

    if args.accounts {
        filters.push(Filter::Accounts(GeyserAccount::accounts()));
    }

    if args.transactions {
        filters.push(Filter::Transactions(GeyserTransaction::transactions()));
    }

    filters
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // config rustls crypto
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    let args = Args::parse();

    let clock = ReceiveTimer::new();

    let mut connector = build_geyser_connector(&args, "solana")?;
    info!("connector: {} has been initialized ...", connector.name());
    connector.connect().await?;
    let filters = build_geyser_filters(&args);
    if filters.is_empty() {
        anyhow::bail!(
            "at least one subscription filter is required: \
             --slots, --accounts, or --transactions"
        );
    }

    let stream = connector.subscribe_to(filters).await?;
    let mut reader = GeyserReader::new(stream, clock.clone());
    while let Some(received) = reader.next().await? {
        let event = normalize_geyser(received)?;
        handle_event(&event);
    }
    connector.disconnect().await?;

    Ok(())
}
