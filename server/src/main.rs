use anyhow::Result;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{self, RecvTimeoutError, SendError};
use std::sync::{Arc, TryLockError, atomic};
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

const IOBUF_SZ: usize = 256;
const CH_RECV_YIELD: Duration = Duration::from_secs(1);
const NET_RECV_YIELD: Duration = Duration::from_secs(1);
const CH_BUFFER_SZ: usize = 10;

type Chunk = Vec<u8>;

struct ConfigEntry {
    id: Uuid,
    ingress_port: u16,
    tunnel: Uuid,
}

struct Client {
    id: Uuid,
    stream: TcpStream,
    tx: Mutex<mpsc::Sender<Chunk>>,
    rx: Mutex<mpsc::Receiver<Chunk>>,
}

struct Task {
    id: Uuid,
    should_exit: AtomicBool,
    clients: Mutex<Vec<Arc<Client>>>,
    tunnel_stream: TcpStream,
    tunnel_tx: Mutex<mpsc::Sender<(Uuid, Chunk)>>,
    tunnel_rx: Mutex<mpsc::Receiver<(Uuid, Chunk)>>,
}

impl Task {
    fn new(id: Uuid, tunnel_stream: TcpStream) -> Self {
        let (tunnel_tx, tunnel_rx) = mpsc::channel::<(Uuid, Chunk)>();

        Self {
            id,
            should_exit: AtomicBool::new(false),
            clients: Mutex::new(Vec::new()),
            tunnel_stream,
            tunnel_tx: Mutex::new(tunnel_tx),
            tunnel_rx: Mutex::new(tunnel_rx),
        }
    }

    fn new_client(&self, id: Uuid, stream: TcpStream) -> Arc<Client> {
        let (tx, rx) = mpsc::channel::<Chunk>();
        let client = Arc::new(Client {
            id,
            stream,
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        });

        let mut clients = self.clients.lock().unwrap();
        clients.push(Arc::clone(&client));
        client
    }

    fn remove_client(&self, id: Uuid) {
        let mut clients = self.clients.lock().unwrap();
        clients.retain(|client| client.id != id);
    }

    // receives client data and sends it to the tunnel channel
    fn drive_client_upstream(&self, client: &Client) {
        let mut client_stream = &client.stream;
        client_stream
            .set_read_timeout(Some(NET_RECV_YIELD))
            .unwrap();

        let mut read_buf = [0u8; IOBUF_SZ];

        loop {
            let n = match client_stream.read(&mut read_buf) {
                Ok(0) => {
                    debug!("stopping because client stream disconnected, removing client");
                    self.remove_client(client.id);
                    let mut client_tx = client.tx.lock().unwrap();
                    close_tx_in_place(&mut client_tx);
                    return;
                }
                Ok(n) => n,
                Err(ref err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        && self.should_exit.load(atomic::Ordering::SeqCst) =>
                {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(err) => panic!("{:?}", err),
            };

            let chunk = read_buf[..n].to_vec();
            match self.tunnel_tx.lock().unwrap().send((client.id, chunk)) {
                Ok(_) => (),
                Err(SendError(_)) => {
                    debug!("stopping due to tunnel rx disconnect");
                    return;
                }
            }
        }
    }

    // sends tunnel channel messages to the upstream endpoint
    fn drive_tunnel_upstream(&self) {
        let mut _tunnel_stream = &self.tunnel_stream;
        let mut tunnel_rx = match self.tunnel_rx.try_lock() {
            Ok(rx) => rx,
            Err(TryLockError::WouldBlock) => panic!("couldn't acquire client rx lock"),
            Err(err @ TryLockError::Poisoned(_)) => panic!("{:?}", err),
        };

        loop {
            match tunnel_rx.recv_timeout(CH_RECV_YIELD) {
                Ok((client_id, chunk)) => {
                    todo!("TODO: send tunnel message {:?} {:?}", client_id, chunk)
                }
                Err(RecvTimeoutError::Timeout)
                    if self.should_exit.load(atomic::Ordering::SeqCst) =>
                {
                    debug!("stopping due to exit signal");
                    close_rx_in_place(&mut tunnel_rx);
                    return;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    debug!("stopping due to tunnel tx signal");
                    return;
                }
            }
        }
    }

    // receives client channel messages and sends them to the client
    fn drive_client_downstream(&self, client: &Client) -> Result<()> {
        let mut client_stream = &client.stream;
        let mut client_rx = match client.rx.try_lock() {
            Ok(rx) => rx,
            Err(TryLockError::WouldBlock) => panic!("couldn't acquire client rx lock"),
            Err(err @ TryLockError::Poisoned(_)) => panic!("{:?}", err),
        };

        loop {
            let chunk = match client_rx.recv_timeout(CH_RECV_YIELD) {
                Ok(chunk) => chunk,
                Err(RecvTimeoutError::Timeout)
                    if self.should_exit.load(atomic::Ordering::SeqCst) =>
                {
                    debug!("stopping due to exit signal");
                    close_rx_in_place(&mut client_rx);
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

        let mut read_buf = [0u8; IOBUF_SZ];

        loop {
            let n = match tunnel_stream.read(&mut read_buf) {
                Ok(0) => {
                    debug!("stopping because tunnel stream disconnected, doing shutdown");
                    self.should_exit
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    let mut tunnel_tx = self.tunnel_tx.lock().unwrap();
                    close_tx_in_place(&mut tunnel_tx);
                    return;
                }
                Ok(n) => n,
                Err(ref err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        && self.should_exit.load(atomic::Ordering::SeqCst) =>
                {
                    debug!("stopping due to exit signal");
                    return;
                }
                Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(err) => panic!("{:?}", err),
            };

            let chunk = read_buf[..n].to_vec();
            let client_id: Uuid = Uuid::new_v4(); // TODO: parse message here
            let client = self
                .clients
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .find(|c| c.id == client_id)
                .unwrap();

            match client.tx.lock().unwrap().send(chunk) {
                Ok(_) => (),
                Err(SendError(_)) => {
                    debug!("stopping due to client rx disconnect");
                    return;
                }
            }
        }
    }
}

fn close_tx_in_place<T>(tx: &mut mpsc::Sender<T>) {
    let (sentinel_tx, _) = mpsc::channel();
    *tx = sentinel_tx;
}

fn close_rx_in_place<T>(rx: &mut mpsc::Receiver<T>) {
    let (_, sentinel_rx) = mpsc::channel();
    *rx = sentinel_rx;
}

fn spawn_tunnel_workers(task: &Arc<Task>) {
    {
        let task = Arc::clone(task);
        std::thread::spawn(move || task.drive_tunnel_upstream());
    }

    {
        let task = Arc::clone(task);
        std::thread::spawn(move || task.drive_tunnel_downstream());
    }
}

fn spawn_client_workers(task: &Arc<Task>, client: &Arc<Client>) {
    {
        let task = Arc::clone(task);
        let client = Arc::clone(client);

        std::thread::spawn(move || task.drive_client_upstream(&client));
    }

    {
        let task = Arc::clone(task);
        let client = Arc::clone(client);

        std::thread::spawn(move || task.drive_client_downstream(&client));
    }
}

struct State {
    config: Vec<ConfigEntry>,
    active: Vec<Task>,
}

fn main() {
    println!("Hello, world!");
}
