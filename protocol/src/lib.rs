use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub enum Message {
    Meta(Meta),
    Incoming { connection: Uuid },
    Accept { connection: Uuid },
    Data { connection: Uuid, data: Vec<u8> },
    Close { connection: Uuid },
}

#[derive(Serialize, Deserialize)]
pub enum Meta {
    CreateRequest {
        kind: TunnelKind,
    },
    CreateResponse {
        tunnel_id: Uuid,
        hostname: String,
        port: u16,
    },
    Attach {
        tunnel_id: Uuid,
    },
    Detach,
}

#[derive(Serialize, Deserialize)]
pub enum TunnelKind {
    Tcp,
}

#[inline(never)]
pub fn encode(w: &mut dyn Write, message: &Message) -> io::Result<()> {
    let data = serde_json::to_vec(message).unwrap();
    let len = data.len() as u16;
    let header = len.to_le_bytes();
    w.write_all(&header).unwrap();
    w.write_all(&data).unwrap();
    Ok(())
}

#[inline(never)]
pub fn decode(r: &mut dyn Read) -> io::Result<Message> {
    let mut header: [u8; 2] = [0; 2];
    r.read_exact(&mut header).unwrap();
    let len = u16::from_le_bytes(header);
    let mut data = vec![0; len as usize];
    r.read_exact(&mut data).unwrap();
    let message = serde_json::from_slice(&data).unwrap();
    Ok(message)
}
