mod connector;
mod timer;

use crate::connector::{Connector, GeyserConnector, ReconnectingPolicy};
use crate::timer::{
    EventEnvelope, EventSource, GeyserEvent, GeyserReader, Reader, ReaderBuilder, ReceiveTimer,
};
use clap::Parser;
use log::info;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // config rustls crypto
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    let args = Args::parse();

    let clock = ReceiveTimer::new();

    let mut connector =
        GeyserConnector::build_connector(args.endpoint, args.policy, args.x_token, "solana")?;
    info!("connector: {} has been initialized ...", connector.name());
    connector.connect().await?;
    let filters = connector.build_filters(args.slots, args.accounts, args.transactions);
    if filters.is_empty() {
        anyhow::bail!(
            "at least one subscription filter is required: \
             --slots, --accounts, or --transactions"
        );
    }

    let stream = connector.subscribe_to(filters).await?;
    let geyser_stream = Box::pin(stream);
    let mut reader: GeyserReader<ReceiveTimer> = ReaderBuilder::new()
        .with_stream(geyser_stream)
        .with_clock(clock)
        .with_source(EventSource::Yellowstone)
        .build()?;

    info!(
        "starting event stream processing - epoch={} seq={} source={}",
        reader.connection_epoch(),
        reader.recv_seq(),
        reader.source(),
    );
    while let Some(received) = reader.next().await? {
        let event = received.normalize()?;
        handle_event(&event);
    }
    connector.disconnect().await?;

    Ok(())
}
