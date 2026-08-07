use async_trait::async_trait;
use iron_core::{
    ClientRequestId, ClusterId, Envelope, EnvelopeFields, MaxMessageBytes, MessageId, MessageType,
    NodeId, ProtocolName, SchemaVersion,
};
use iron_protocol::{
    Action, ClientRequest, DurableRecord, Event, Protocol, ProtocolError, RecoveredRecord,
    ReplyToClient, Transition,
};
use iron_testkit::{
    FaultAction, FaultPredicate, FaultRule, SimulatedNetwork, SimulatedTransport,
    exercise_at_least_once_contract,
};

fn opaque(last_byte: u8) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[15] = last_byte;
    bytes
}

fn envelope() -> Envelope {
    Envelope::new_v1(
        EnvelopeFields::initial(
            ClusterId::from_bytes(opaque(1)).expect("cluster ID is nonzero"),
            NodeId::parse("node-a").expect("source node is valid"),
            NodeId::parse("node-b").expect("destination node is valid"),
            MessageId::from_bytes(opaque(2)).expect("message ID is nonzero"),
            None,
            ProtocolName::parse("foundation.echo").expect("protocol name is valid"),
            MessageType::parse("request").expect("message type is valid"),
            SchemaVersion::new(1).expect("schema version is nonzero"),
            b"command".to_vec(),
        ),
        MaxMessageBytes::new(1_024).expect("message bound is valid"),
    )
    .expect("fixture envelope is within its bound")
}

#[tokio::test(flavor = "current_thread")]
async fn scripted_duplicate_delivery_preserves_transport_and_envelope_contracts() {
    let mut network = SimulatedNetwork::new();
    network.add_fault(FaultRule::new(
        FaultPredicate {
            ordinal: Some(1),
            ..FaultPredicate::default()
        },
        FaultAction::Duplicate {
            additional_copies: 1,
        },
        true,
    ));
    let transport = SimulatedTransport::new(network);
    let sent = envelope();
    exercise_at_least_once_contract(&transport, sent.clone())
        .await
        .expect("transport accepts repeated attempts without deduplicating");

    let shared = transport.network();
    let mut network = shared.lock().expect("test does not poison network mutex");
    let mut delivered = Vec::new();
    while let Some(next) = network.pop_delivery().expect("driver invariants hold") {
        delivered.push(next.into_event());
    }

    assert_eq!(
        delivered.len(),
        3,
        "first attempt duplicates, second is normal"
    );
    assert!(delivered.iter().all(|copy| {
        copy.message_id() == sent.message_id()
            && copy.delivery_attempt() == sent.delivery_attempt()
            && copy.semantic_fingerprint() == sent.semantic_fingerprint()
    }));
}

#[derive(Clone)]
struct EchoProtocol {
    name: ProtocolName,
}

#[async_trait]
impl Protocol for EchoProtocol {
    fn name(&self) -> &ProtocolName {
        &self.name
    }

    async fn recover(&mut self, _records: &[RecoveredRecord]) -> Result<(), ProtocolError> {
        Ok(())
    }

    async fn handle(&mut self, event: Event) -> Result<Transition, ProtocolError> {
        match event {
            Event::ClientRequest(request) => {
                let record = DurableRecord::new(
                    1_024,
                    request.schema_version(),
                    request.command().to_vec(),
                )?;
                Ok(Transition::durable(
                    record,
                    vec![Action::ReplyToClient(ReplyToClient::new(
                        request.request_id(),
                        request.command().to_vec(),
                    ))],
                ))
            }
            _ => Ok(Transition::no_op()),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn identical_protocol_state_and_event_produce_identical_transition() {
    let name = ProtocolName::parse("foundation.echo").expect("protocol name is valid");
    let mut left = EchoProtocol { name: name.clone() };
    let mut right = EchoProtocol { name };
    let event = Event::ClientRequest(ClientRequest::new(
        ClientRequestId::from_bytes(opaque(3)).expect("request ID is nonzero"),
        SchemaVersion::new(1).expect("schema version is nonzero"),
        b"deterministic".to_vec(),
    ));

    let left_transition = left
        .handle(event.clone())
        .await
        .expect("fixture handles event");
    let right_transition = right.handle(event).await.expect("fixture handles event");
    assert_eq!(left_transition, right_transition);
    assert_eq!(
        left_transition
            .durable_record()
            .expect("state change is durable")
            .payload()
            .as_ref(),
        b"deterministic"
    );
}
