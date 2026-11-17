use crate::{
    receiver::{receiver, ReceiverError},
    sender::{self, sender, MdnsMsg},
    socket::{SocketError, Sockets},
    updater::updater,
    Discoverer,
};
use acto::{AcTokioRuntime, ActoCell, ActoId, ActoInput, ActoRef};
use hickory_proto::rr::Name;
use std::{
    collections::HashMap,
    mem::replace,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};
use tokio::net::UdpSocket;

type GuardianCell = ActoCell<Input, AcTokioRuntime, Result<(), ReceiverError>>;

/// How long to wait before replacing the socket of a failed receiver, so that a
/// socket which keeps failing cannot spin the guardian.
const RECEIVER_RESTART_DELAY: Duration = Duration::from_secs(1);

pub enum Input {
    RemoveAll,
    RemovePort(u16),
    RemoveAddr(IpAddr),
    AddAddr(u16, Vec<IpAddr>),
    SetTxt(String, Option<String>),
    RemoveTxt(String),
    AddInterface(IpAddr),
    RemoveInterface(IpAddr),
    RestartReceiver(ReceiverSocket),
}

/// The socket a receiver listens on.
#[derive(Clone, Copy, Debug)]
pub enum ReceiverSocket {
    V4,
    V6,
    Interface(Ipv4Addr),
}

impl ReceiverSocket {
    fn actor_name(self) -> String {
        match self {
            ReceiverSocket::V4 => "receiver_v4".to_string(),
            ReceiverSocket::V6 => "receiver_v6".to_string(),
            ReceiverSocket::Interface(addr) => format!("receiver_interface_{}", addr),
        }
    }

    /// Binds a fresh socket in place of this one, or returns `None` if it is
    /// gone for good (a removed interface).
    fn rebind(self, sockets: &Sockets) -> Result<Option<Arc<UdpSocket>>, SocketError> {
        match self {
            ReceiverSocket::V4 => sockets.rebind_v4().map(Some),
            ReceiverSocket::V6 => sockets.rebind_v6().map(Some),
            ReceiverSocket::Interface(addr) => sockets.rebind_interface_v4(addr),
        }
    }
}

/// Spawns receivers and remembers which socket each one listens on, so that a
/// failed one can be restarted on a fresh socket.
struct Receivers {
    sockets: HashMap<ActoId, ReceiverSocket>,
    service_name: Name,
    sender: ActoRef<MdnsMsg>,
}

impl Receivers {
    fn spawn(
        &mut self,
        ctx: &mut GuardianCell,
        which: ReceiverSocket,
        socket: Arc<UdpSocket>,
    ) -> ActoRef<()> {
        let service_name = self.service_name.clone();
        let sender = self.sender.clone();
        let receiver_ref = ctx.spawn_supervised(&which.actor_name(), move |ctx| {
            receiver(ctx, service_name, socket, sender)
        });
        self.sockets.insert(receiver_ref.id(), which);
        receiver_ref
    }
}

pub async fn guardian(
    mut ctx: GuardianCell,
    mut discoverer: Discoverer,
    sockets: Sockets,
    service_name: Name,
) {
    let callback = replace(&mut discoverer.callback, Box::new(|_, _| {}));
    let tau = discoverer.tau;
    let phi = discoverer.phi;
    let upd_ref = ctx.supervise(
        ctx.spawn("updater", move |ctx| updater(ctx, tau, phi, callback))
            .map_handle(Ok),
    );

    let sockets2 = sockets.clone();
    let sn = service_name.clone();
    let snd_ref = ctx.supervise(
        ctx.spawn("sender", move |ctx| {
            sender(ctx, sockets, upd_ref, discoverer, sn)
        })
        .map_handle(Ok),
    );

    let mut receivers = Receivers {
        sockets: HashMap::new(),
        service_name,
        sender: snd_ref.clone(),
    };

    if let Some(v4) = sockets2.v4() {
        receivers.spawn(&mut ctx, ReceiverSocket::V4, v4);
    }

    if let Some(v6) = sockets2.v6() {
        receivers.spawn(&mut ctx, ReceiverSocket::V6, v6);
    }

    // Track interface receivers so we can stop them when interfaces are removed
    let mut interface_receivers: HashMap<IpAddr, ActoRef<()>> = HashMap::new();

    // Start receivers for initial interface sockets
    let initial_interfaces = sockets2.get_all_interface_addresses_v4();
    for addr in initial_interfaces {
        if let Some(socket) = sockets2.get_interface_socket_v4(addr) {
            let receiver_ref = receivers.spawn(&mut ctx, ReceiverSocket::Interface(addr), socket);
            interface_receivers.insert(IpAddr::V4(addr), receiver_ref);
            tracing::info!("Started receiver for initial interface {}", addr);
        }
    }

    // only stop when a supervised actor stops
    loop {
        let msg = ctx.recv().await;
        match msg {
            ActoInput::NoMoreSenders => {}
            ActoInput::Supervision { id, name, result } => {
                match result {
                    Ok(Ok(_)) => tracing::warn!("actor {:?} ({}) stopped", id, name),
                    Ok(Err(e)) => {
                        tracing::warn!("actor {:?} ({}) failed: {}", id, name, e);
                        // The OS can tear a socket down underneath us (iOS does
                        // to a suspended app's): bind a fresh one instead of
                        // giving up on discovery.
                        if let Some(which) = receivers.sockets.remove(&id) {
                            restart_receiver_later(&mut ctx, which);
                            continue;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("actor {:?} ({}) aborted: {}", id, name, e);
                    }
                }
                break;
            }
            ActoInput::Message(msg) => match &msg {
                Input::AddInterface(addr) => {
                    if let IpAddr::V4(ipv4) = addr {
                        if let Err(e) = sockets2.add_interface_v4(*ipv4) {
                            tracing::warn!("Failed to add interface {}: {}", addr, e);
                        } else {
                            // Start a receiver for the new interface socket
                            if let Some(socket) = sockets2.get_interface_socket_v4(*ipv4) {
                                let receiver_ref = receivers.spawn(
                                    &mut ctx,
                                    ReceiverSocket::Interface(*ipv4),
                                    socket,
                                );
                                interface_receivers.insert(*addr, receiver_ref);
                                tracing::info!("Started receiver for interface {}", addr);
                            }
                        }
                    }
                }
                Input::RemoveInterface(addr) => {
                    if let IpAddr::V4(ipv4) = addr {
                        sockets2.remove_interface_v4(*ipv4);
                        // Remove the receiver reference for this interface
                        if interface_receivers.remove(addr).is_some() {
                            tracing::info!("Removed receiver reference for interface {}", addr);
                        }
                    }
                }
                Input::RestartReceiver(which) => match which.rebind(&sockets2) {
                    Ok(Some(socket)) => {
                        let receiver_ref = receivers.spawn(&mut ctx, *which, socket);
                        if let ReceiverSocket::Interface(addr) = which {
                            interface_receivers.insert(IpAddr::V4(*addr), receiver_ref);
                        }
                        // Warn rather than info: it is rare, and what tells a recovery apart
                        // from a discoverer that stopped for good in logs that only keep warnings.
                        tracing::warn!("Restarted {} on a fresh socket", which.actor_name());
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!("Failed to rebind {}: {}", which.actor_name(), e);
                        restart_receiver_later(&mut ctx, *which);
                    }
                },
                _ => {
                    snd_ref.send(sender::MdnsMsg::Update(msg));
                }
            },
        }
    }
}

fn restart_receiver_later(ctx: &mut GuardianCell, which: ReceiverSocket) {
    let me = ctx.me();
    tokio::spawn(async move {
        tokio::time::sleep(RECEIVER_RESTART_DELAY).await;
        me.send(Input::RestartReceiver(which));
    });
}
