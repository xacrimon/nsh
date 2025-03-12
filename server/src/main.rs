mod channel;

use anyhow::Result;
use channel::{Channel, RecvTimeoutError, SendError, predicate};
use std::io::{self, Read, Write};
use std::net::{self, TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, TryLockError, atomic};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

const CLIENT_IOBUF_SZ: usize = 256;
const CLIENT_RX_QUEUE_LEN_LIMIT: usize = 16;
const CLIENT_TX_QUEUE_SZ_LIMIT: usize = 4096;
const ACCEPT_INTERVAL: Duration = Duration::from_millis(50);
const CH_RECV_YIELD: Duration = Duration::from_millis(50);
const NET_RECV_YIELD: Duration = Duration::from_millis(50);

fn can_queue_additional_tunnel(client: Uuid) -> predicate!((Uuid, Chunk)) {
    move |queued| queued.filter(|(id, _)| *id == client).count() <= CLIENT_RX_QUEUE_LEN_LIMIT
}

fn can_queue_additional_client() -> predicate!(Chunk) {
    |queued| queued.map(|c| c.len()).sum::<usize>() <= CLIENT_TX_QUEUE_SZ_LIMIT
}

type Chunk = Vec<u8>;

struct TunnelConfig {
    id: Uuid,
    ingress_port: u16,
}

struct Client {
    id: Uuid,
    stream: TcpStream,
    ch: Channel<Chunk>,
}

struct Tunnel {
    id: Uuid,
    closed: AtomicBool,
    clients: Mutex<Vec<Arc<Client>>>,
    tunnel_stream: TcpStream,
    tunnel_ch: Channel<(Uuid, Chunk)>,
}

impl Tunnel {
    fn new(id: Uuid, tunnel_stream: TcpStream) -> Arc<Self> {
        let tunnel_ch = Channel::new();
        let tunnel = Arc::new(Self {
            id,
            closed: AtomicBool::new(false),
            clients: Mutex::new(Vec::new()),
            tunnel_stream,
            tunnel_ch,
        });

        spawn_tunnel_workers(&tunnel);
        tunnel
    }

    fn new_client(&self, id: Uuid, stream: TcpStream) -> Arc<Client> {
        let ch = Channel::new();
        let client = Arc::new(Client { id, stream, ch });

        let mut clients = self.clients.lock().unwrap();
        clients.push(Arc::clone(&client));
        client
    }

    fn remove_client(&self, id: Uuid) {
        let mut clients = self.clients.lock().unwrap();
        clients.retain(|client| client.id != id);
    }

    fn drive_listener(self: &Arc<Self>, config: &TunnelConfig) {
        let addr = format!("localhost:{}", config.ingress_port);
        let listener = TcpListener::bind(&addr).unwrap();
        listener.set_nonblocking(true).unwrap();

        loop {
            let (stream, addr) = match listener.accept() {
                Ok((stream, addr)) => (stream, addr),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock && self.is_closed() => {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_INTERVAL);
                    continue;
                }
                Err(err) => panic!("{:?}", err),
            };

            debug!("accepted new client connection from {:?}", addr);
            let id = Uuid::new_v4();
            let client = self.new_client(id, stream);
            spawn_client_workers(self, client);
        }
    }

    // receives client data and sends it to the tunnel channel
    fn drive_client_upstream(&self, client: &Client) {
        let mut client_stream = &client.stream;
        client_stream
            .set_read_timeout(Some(NET_RECV_YIELD))
            .unwrap();

        let mut read_buf = [0u8; CLIENT_IOBUF_SZ];

        loop {
            let n = match client_stream.read(&mut read_buf) {
                Ok(0) => {
                    debug!("stopping because client stream disconnected, removing client");
                    self.remove_client(client.id);
                    client.ch.close();
                    return;
                }
                Ok(n) => n,
                Err(ref err) if err.kind() == io::ErrorKind::TimedOut && self.is_closed() => {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(ref err) if err.kind() == io::ErrorKind::TimedOut => continue,
                Err(err) => panic!("{:?}", err),
            };

            let chunk = read_buf[..n].to_vec();
            let predicate = can_queue_additional_tunnel(client.id);
            match self.tunnel_ch.send_when((client.id, chunk), predicate) {
                Ok(_) => (),
                Err(SendError) => {
                    debug!("stopping due to tunnel rx disconnect");
                    return;
                }
            }
        }
    }

    // sends tunnel channel messages to the upstream endpoint
    fn drive_tunnel_upstream(&self) {
        let mut tunnel_stream = &self.tunnel_stream;

        loop {
            let (client_id, chunk) = match self.tunnel_ch.recv_timeout(CH_RECV_YIELD) {
                Ok((client_id, chunk)) => (client_id, chunk),
                Err(RecvTimeoutError::Timeout) if self.is_closed() => {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    debug!("stopping due to tunnel tx signal");
                    return;
                }
            };

            let message = protocol::Message::Data {
                connection: client_id,
                data: chunk,
            };

            match protocol::encode(&mut tunnel_stream, &message) {
                Ok(_) => (),
                Err(err) => {
                    debug!(
                        "stopping because tunnel stream disconnected with err {:?}, doing shutdown",
                        err
                    );

                    self.try_close();
                    return;
                }
            }
        }
    }

    // receives client channel messages and sends them to the client
    fn drive_client_downstream(&self, client: &Client) -> Result<()> {
        let mut client_stream = &client.stream;

        loop {
            let chunk = match client.ch.recv_timeout(CH_RECV_YIELD) {
                Ok(chunk) => chunk,
                Err(RecvTimeoutError::Timeout) if self.is_closed() => {
                    debug!("stopping due to exit signal");
                    self.remove_client(client.id);
                    client.ch.close();
                    return Ok(());
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    debug!("stopping due to client tx disconnect");
                    return Ok(());
                }
            };

            client_stream.write_all(&chunk)?;
        }
    }

    // reads messages from upstream endpoint and routes them to the appropriate client channel
    fn drive_tunnel_downstream(&self) {
        let mut tunnel_stream = &self.tunnel_stream;
        tunnel_stream
            .set_read_timeout(Some(NET_RECV_YIELD))
            .unwrap();

        loop {
            let message = match protocol::decode(&mut tunnel_stream) {
                Ok(message) => message,
                Err(ref err) if err.kind() == io::ErrorKind::TimedOut && self.is_closed() => {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(ref err) if err.kind() == io::ErrorKind::TimedOut => continue,
                Err(err) => panic!("{:?}", err),
            };

            let (client_id, chunk) = if let protocol::Message::Data { connection, data } = message {
                (connection, data)
            } else {
                panic!()
            };

            let client = self
                .clients
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .find(|c| c.id == client_id)
                .unwrap();

            let predicate = can_queue_additional_client();
            match client.ch.send_when(chunk, predicate) {
                Ok(_) => (),
                Err(SendError) => {
                    debug!("stopping due to client rx disconnect");
                    return;
                }
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(atomic::Ordering::SeqCst)
    }

    fn try_close(&self) {
        let was_closed = self.closed.swap(true, atomic::Ordering::SeqCst);
        if !was_closed {
            debug!("closing tunnel");
            self.tunnel_ch.close();
            self.tunnel_stream.shutdown(net::Shutdown::Both).unwrap();
        }
    }
}

fn assert_exclusive<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    match lock.try_lock() {
        Ok(rx) => rx,
        Err(TryLockError::WouldBlock) => panic!("couldn't acquire client rx lock"),
        Err(err @ TryLockError::Poisoned(_)) => panic!("{:?}", err),
    }
}

fn spawn_tunnel_workers(tunnel: &Arc<Tunnel>) {
    {
        let tunnel = Arc::clone(tunnel);
        std::thread::spawn(move || tunnel.drive_tunnel_upstream());
    }

    {
        let tunnel = Arc::clone(tunnel);
        std::thread::spawn(move || tunnel.drive_tunnel_downstream());
    }
}

fn spawn_client_workers(tunnel: &Arc<Tunnel>, client: Arc<Client>) {
    {
        let tunnel = Arc::clone(tunnel);
        let client = Arc::clone(&client);

        std::thread::spawn(move || tunnel.drive_client_upstream(&client));
    }

    let tunnel = Arc::clone(tunnel);
    std::thread::spawn(move || tunnel.drive_client_downstream(&client));
}

struct State {
    waiting: Vec<Uuid>,
    tunnels: Vec<Tunnel>,
}

fn main() {
    println!("Hello, world!");
}
