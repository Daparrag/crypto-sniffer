use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::time::Instant;
use thiserror::Error;

use yellowstone_grpc_proto::prelude::{
    SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateSlot, SubscribeUpdateTransaction,
    subscribe_update::UpdateOneof,
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

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Error)]
pub enum GeyserReaderError {
    #[error("Yellowstone stream error : {0}")]
    InvalidStream(#[from] Status),
    #[error("Invalid Event ")]
    InvalidEvent,
    #[error("Reader not fully initialized")]
    BuildError(String),
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
/// Implement a dedicated received event for SubscribeUdate Type
/// This method should not exists for other types
impl ReceivedEvent<SubscribeUpdate> {
    pub fn normalize(self) -> Result<EventEnvelope<GeyserEvent>, GeyserReaderError> {
        let (source, connection_epoch, recv_seq, received_mono_ns, payload) = self.into_parts();
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
}

pub type GeyserStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

pub trait Reader<C>
where
    C: Clock,
{
    type Event;
    type Error;

    async fn next(&mut self) -> Result<Option<Self::Event>, Self::Error>;
    fn connection_epoch(&self) -> u64;
    fn recv_seq(&self) -> u64;
    fn source(&self) -> EventSource;
}

pub struct StreamReader<S, C>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
    C: Clock,
{
    stream: S,
    clock: C,
    source: EventSource,
    recv_seq: u64,
    connection_epoch: u64,
}

impl<S, C> StreamReader<S, C>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
    C: Clock,
{
    pub fn new(stream: S, clock: C, source: EventSource) -> Self {
        Self {
            stream,
            clock,
            source,
            recv_seq: 0,
            connection_epoch: 0,
        }
    }
}

impl<S, C> Reader<C> for StreamReader<S, C>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
    C: Clock,
{
    type Event = ReceivedEvent<SubscribeUpdate>;
    type Error = GeyserReaderError;

    async fn next(&mut self) -> Result<Option<ReceivedEvent<SubscribeUpdate>>, GeyserReaderError> {
        use futures::StreamExt;

        let message = self.stream.next().await;
        let received_mono_ns = self.clock.now_ns();

        match message {
            Some(Ok(update)) => {
                self.recv_seq += 1;
                Ok(Some(ReceivedEvent::new(
                    self.source,
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
    fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }

    fn recv_seq(&self) -> u64 {
        self.recv_seq
    }

    fn source(&self) -> EventSource {
        self.source
    }
}

pub type GeyserReader<C> = StreamReader<GeyserStream, C>;

pub struct ReaderBuilder<C>
where
    C: Clock,
{
    stream: Option<GeyserStream>,
    clock: Option<C>,
    source: Option<EventSource>,
}

impl<C> ReaderBuilder<C>
where
    C: Clock,
{
    pub fn new() -> Self {
        Self {
            stream: None,
            clock: None,
            source: None,
        }
    }

    pub fn with_stream(mut self, stream: GeyserStream) -> Self {
        self.stream = Some(stream);
        self
    }

    pub fn with_clock(mut self, clock: C) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn with_source(mut self, source: EventSource) -> Self {
        self.source = Some(source);
        self
    }

    pub fn build(self) -> Result<GeyserReader<C>, GeyserReaderError> {
        let stream = self
            .stream
            .ok_or_else(|| GeyserReaderError::BuildError("stream not set".into()))?;

        let clock = self
            .clock
            .ok_or_else(|| GeyserReaderError::BuildError("clock not set".into()))?;

        let source = self.source.unwrap_or(EventSource::Yellowstone);

        Ok(StreamReader::new(stream, clock, source))
    }
}

impl<C> Default for ReaderBuilder<C>
where
    C: Clock,
{
    fn default() -> Self {
        Self::new()
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
    /*
        pub fn into_parts(self) -> (EventSource, u64, u64, u64, T) {
            (
                self.source,
                self.connection_epoch,
                self.recv_seq,
                self.received_mono_ns,
                self.event,
            )
        }
    */

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

    use std::time::Duration;
    use yellowstone_grpc_proto::tonic::Status;

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

    #[test]
    fn receive_timer_is_monotonic() {
        let timer = ReceiveTimer::new();
        let t1 = timer.now_ns();
        std::thread::sleep(Duration::from_micros(100));
        let t2 = timer.now_ns();
        assert!(t2 > t1, "Timer must be strictly monotonic");
    }

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

        let normalized = received.normalize().unwrap();

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

    #[test]
    fn envelope_accessor_consistency() {
        let env = EventEnvelope::new(
            EventSource::Yellowstone,
            1,
            42,
            999,
            GeyserEvent::Slot(SubscribeUpdateSlot {
                slot: 100,
                ..Default::default()
            }),
        );

        assert_eq!(env.source(), EventSource::Yellowstone);
        assert_eq!(env.connection_epoch(), 1);
        assert_eq!(env.recv_seq(), 42);
        assert_eq!(env.received_mono_ns(), 999);
    }

    #[test]
    fn received_event_round_trip() {
        let original = ReceivedEvent::new(EventSource::Yellowstone, 10, 50, 12345, "test_payload");

        let (s, e, seq, ts, p) = original.into_parts();

        let reconstructed = ReceivedEvent::new(s, e, seq, ts, p);

        assert_eq!(reconstructed.source, s);
        assert_eq!(reconstructed.recv_seq, seq);
    }

    #[test]
    fn error_messages_are_informative() {
        let err = GeyserReaderError::InvalidEvent;
        assert_eq!(err.to_string(), "Invalid Event "); // Note: trailing space in your error!

        let status = Status::internal("test");
        let stream_err = GeyserReaderError::InvalidStream(status);
        assert!(stream_err.to_string().contains("Yellowstone stream error"));
    }

    #[test]
    fn builder_with_all_fields() {
        let stream: GeyserStream = Box::pin(futures::stream::iter(vec![Ok(make_slot_update(100))]));
        let clock = MockClock { now_ns: 1000 };

        let reader = ReaderBuilder::new()
            .with_stream(stream)
            .with_clock(clock)
            .with_source(EventSource::Yellowstone)
            .build();

        assert!(reader.is_ok());
        let reader = reader.unwrap();
        assert_eq!(reader.source(), EventSource::Yellowstone);
        assert_eq!(reader.recv_seq(), 0);
    }

    #[test]
    fn builder_with_defaults() {
        let stream: GeyserStream = Box::pin(futures::stream::iter(vec![Ok(make_slot_update(100))]));
        let clock = MockClock { now_ns: 1000 };

        let reader = ReaderBuilder::new()
            .with_stream(stream)
            .with_clock(clock)
            .build();

        assert!(reader.is_ok());
        let reader = reader.unwrap();
        assert_eq!(reader.source(), EventSource::Yellowstone);
    }

    #[test]
    fn builder_missing_stream() {
        let clock = MockClock { now_ns: 1000 };

        let result = ReaderBuilder::new().with_clock(clock).build();

        assert!(result.is_err());
    }

    #[test]
    fn builder_missing_clock() {
        let stream: GeyserStream = Box::pin(futures::stream::iter(vec![Ok(make_slot_update(100))]));

        let result = ReaderBuilder::<ReceiveTimer>::new()
            .with_stream(stream)
            .build();

        assert!(result.is_err());
    }
}
