use futures::StreamExt;
use std::time::Instant;
use thiserror::Error;
use yellowstone_grpc_client::GeyserStream;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
};

use yellowstone_grpc_proto::tonic::Status;

pub trait Clock: Clone + Send + Sync + 'static {
    type Instant: Copy;
    fn now_ns(&self) -> u64;

    //fn elapsed_since(&self, start: Self::Instant) -> Duration;
}

#[derive(Debug, Clone)]
pub struct ReceiveTimer {
    reference: Instant,
}

impl ReceiveTimer {
    pub fn new() -> Self {
        Self {
            reference: Instant::now(),
        }
    }
}

impl Clock for ReceiveTimer {
    type Instant = Instant;
    fn now_ns(&self) -> u64 {
        let elapsed = self.reference.elapsed();
        elapsed
            .as_secs()
            .saturating_mul(1_000_000_000)
            .saturating_add(u64::from(elapsed.subsec_nanos()))
    }
    /*
        fn elapsed_since(&self, start: Self::Instant) -> Duration {
            start.elapsed()
        }
    */
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    Yellowstone,
}

#[derive(Debug, Clone, Error)]
pub enum GeyserReaderError {
    #[error("Yellowstone stream error : {0}")]
    InvalidStream(#[from] Status),
    #[error("Invalid Event ")]
    InvalidEvent,
}

#[derive(Debug)]
pub struct ReceivedEvent<T> {
    source: EventSource,
    connection_epoch: u64,
    recv_seq: u64,
    received_mono_ns: u64,
    payload: T,
}

impl<T> ReceivedEvent<T> {
    pub fn new(
        source: EventSource,
        connection_epoch: u64,
        recv_seq: u64,
        received_mono_ns: u64,
        payload: T,
    ) -> Self {
        Self {
            source,
            connection_epoch,
            recv_seq,
            received_mono_ns,
            payload,
        }
    }

    pub fn into_parts(self) -> (EventSource, u64, u64, u64, T) {
        (
            self.source,
            self.connection_epoch,
            self.recv_seq,
            self.received_mono_ns,
            self.payload,
        )
    }
}

pub trait Reader<C> {
    type Event;
    type Error;
    type StreamType;
    fn new(stream: Self::StreamType, clock: C) -> Self;
    async fn next(&mut self) -> Result<Option<Self::Event>, Self::Error>;
}

pub struct GeyserReader<C>
where
    C: Clock,
{
    stream: GeyserStream,
    clock: C,
    recv_seq: u64,
    connection_epoch: u64,
}

impl<C> Reader<C> for GeyserReader<C>
where
    C: Clock,
{
    type Event = ReceivedEvent<SubscribeUpdate>;
    type Error = GeyserReaderError;
    type StreamType = GeyserStream;

    fn new(stream: Self::StreamType, clock: C) -> Self {
        Self {
            stream,
            clock,
            recv_seq: 0,
            connection_epoch: 0,
        }
    }

    async fn next(&mut self) -> Result<Option<Self::Event>, Self::Error> {
        let message = self.stream.next().await;

        // IMPORTANT:
        // timestamp immediately after the stream wakes us up.
        let received_mono_ns = self.clock.now_ns();

        match message {
            Some(Ok(update)) => {
                self.recv_seq += 1;

                Ok(Some(ReceivedEvent::new(
                    EventSource::Yellowstone,
                    self.connection_epoch,
                    self.recv_seq,
                    received_mono_ns,
                    update,
                )))
            }

            Some(Err(status)) => Err(GeyserReaderError::InvalidStream(status)),

            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GeyserEvent {
    Slot(SubscribeUpdateSlot),
    Transactions(Box<SubscribeUpdateTransaction>),
    AccountUpdate(SubscribeUpdateAccount),
}

#[derive(Debug, Clone)]
pub struct EventEnvelope<T> {
    source: EventSource,
    connection_epoch: u64,
    recv_seq: u64,
    received_mono_ns: u64,
    event: T,
}

impl<T> EventEnvelope<T> {
    pub fn source(&self) -> EventSource {
        self.source
    }

    pub fn recv_seq(&self) -> u64 {
        self.recv_seq
    }

    pub fn received_mono_ns(&self) -> u64 {
        self.received_mono_ns
    }

    pub fn event(&self) -> &T {
        &self.event
    }

    pub fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }

    pub fn new(
        source: EventSource,
        connection_epoch: u64,
        recv_seq: u64,
        received_mono_ns: u64,
        event: T,
    ) -> Self {
        Self {
            source,
            connection_epoch,
            recv_seq,
            received_mono_ns,
            event,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
    #[derive(Clone)]
    struct MockClock {
        now_ns: u64,
    }

    impl Clock for MockClock {
        type Instant = u64;
        fn now_ns(&self) -> u64 {
            self.now_ns
        }
    }

    fn make_slot_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn normalize_geyser(
        received: ReceivedEvent<SubscribeUpdate>,
    ) -> Result<EventEnvelope<GeyserEvent>, GeyserReaderError> {
        let ReceivedEvent {
            source,
            connection_epoch,
            recv_seq,
            received_mono_ns,
            payload,
        } = received;

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

        Ok(EventEnvelope {
            source,
            connection_epoch,
            recv_seq,
            received_mono_ns,
            event,
        })
    }
    //fn make_account_update(slot: u64) -> SubscribeUpdate;
    //fn make_transaction_update(slot: u64) -> SubscribeUpdate;

    #[test]
    fn mock_clock_returns_configured_time() {
        let clock = MockClock {
            now_ns: 123_456_789,
        };

        assert_eq!(clock.now_ns(), 123_456_789);
    }

    #[test]
    fn normalization_preserves_receive_timestamp() {
        let update = make_slot_update(123);

        let received = ReceivedEvent::new(EventSource::Yellowstone, 0, 42, 123_456_789, update);

        let normalized = normalize_geyser(received).unwrap();

        assert_eq!(normalized.received_mono_ns(), 123_456_789);

        assert_eq!(normalized.recv_seq(), 42);
    }
    #[test]
    fn received_event_preserves_timestamp() {
        let clock = MockClock {
            now_ns: 123_456_789,
        };

        let received =
            ReceivedEvent::new(EventSource::Yellowstone, 1, 42, clock.now_ns(), "payload");

        let (source, epoch, seq, timestamp, payload) = received.into_parts();

        assert_eq!(source, EventSource::Yellowstone);
        assert_eq!(epoch, 1);
        assert_eq!(seq, 42);
        assert_eq!(timestamp, 123_456_789);
        assert_eq!(payload, "payload");
    }
}
