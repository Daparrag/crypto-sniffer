mod connector;

use clap::Parser;
use connector::{
    Connector, ConnectorConfig, Filter, GeyserAccount, GeyserConnector, GeyserSlots,
    GeyserTransaction, ReconnectingPolicy,
};
use futures::StreamExt;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // config rustls crypto
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let args = Args::parse();

    let mut config = ConnectorConfig::new(args.endpoint)?.with_policy(args.policy);

    if let Some(token) = args.x_token {
        config = config.with_token(token);
    }
    let mut connector = GeyserConnector::new("solana").with_config(config);

    connector.connect().await?;

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

    if filters.is_empty() {
        anyhow::bail!(
            "at least one subscription filter is required: \
             --slots, --accounts, or --transactions"
        );
    }

    let mut stream = connector.subscribe_to(filters).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(update) => {
                println!("{update:?}");
            }
            Err(status) => {
                eprintln!("stream error: {status}");
                break;
            }
        }
    }

    connector.disconnect().await?;

    Ok(())
}
