pub mod request;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Result, anyhow};
use log::debug;

use crate::conn::request::{Request, RequestData};

/// Address both sockets are bound to
const BIND_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
/// Control socket port, shared by every peer
pub const CONTROL_SOCKET_PORT: u16 = 5000;
/// Data socket port, shared by every peer
pub const DATA_SOCKET_PORT: u16 = 5001;
/// Maximum UDP datagram payload size
const DATAGRAM_SIZE: usize = 65_535;
/// Data socket read timeout
const DATA_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared peer connection reference
pub type PeerConnRef = Arc<PeerConn>;

/// Centralized socket connections (data and control)
pub struct PeerConn {
    /// Control socket with signalling requests
    control_socket: UdpSocket,
    /// Data socket with file content and file tree transfers
    data_socket: Mutex<UdpSocket>,
}

impl PeerConn {
    /// Binds sockets to their ports
    pub fn bind() -> Result<Self> {
        Self::new(
            UdpSocket::bind(SocketAddr::new(BIND_ADDR, CONTROL_SOCKET_PORT))?,
            UdpSocket::bind(SocketAddr::new(BIND_ADDR, DATA_SOCKET_PORT))?,
        )
    }

    pub fn new(control_socket: UdpSocket, data_socket: UdpSocket) -> Result<Self> {
        data_socket.set_read_timeout(Some(DATA_READ_TIMEOUT))?;

        Ok(Self {
            control_socket,
            data_socket: Mutex::new(data_socket),
        })
    }

    /// Receives the next control request
    pub fn recv_control(&self) -> Result<(SocketAddr, Result<RequestData>)> {
        let mut buf = vec![0; DATAGRAM_SIZE];
        let (received, src) = self.control_socket.recv_from(&mut buf)?;

        Ok((src, RequestData::try_from(&buf[..received].to_vec())))
    }

    /// Sends a request to the peer control socket
    pub fn send_control(&self, data: RequestData, peer: IpAddr) -> Result<()> {
        self.reply_control(data, SocketAddr::new(peer, CONTROL_SOCKET_PORT))
    }

    /// Sends a request back to the exact address a control request came from
    pub fn reply_control(&self, data: RequestData, addr: SocketAddr) -> Result<()> {
        let bytes: Box<[u8]> = Request { data }.into();
        debug!(
            "Sending control request to {addr} ({} bytes): {:?}",
            bytes.len(),
            String::from_utf8(bytes.to_vec())
        );
        self.control_socket.send_to(&bytes, addr)?;

        Ok(())
    }

    /// Acquires exclusive access to the data socket
    pub fn data_channel(&self) -> Result<DataChannel<'_>> {
        let socket = self
            .data_socket
            .lock()
            .map_err(|e| anyhow!("Could not acquire data socket lock: {e}"))?;

        Ok(DataChannel {
            socket,
            buf: vec![0; DATAGRAM_SIZE],
        })
    }
}

/// Exclusive access to the data socket, held for the duration of a transfer
pub struct DataChannel<'a> {
    /// Locked data socket
    socket: MutexGuard<'a, UdpSocket>,
    /// Reusable receive buffer
    buf: Vec<u8>,
}

impl DataChannel<'_> {
    /// Sends a request to the peer data socket
    pub fn send(&self, data: RequestData, peer: IpAddr) -> Result<()> {
        let addr = SocketAddr::new(peer, DATA_SOCKET_PORT);
        let bytes: Box<[u8]> = Request { data }.into();
        debug!("Sending data request to {addr} ({} bytes)", bytes.len());
        self.socket.send_to(&bytes, addr)?;

        Ok(())
    }

    /// Receives the next request from the data socket
    pub fn recv(&mut self) -> Result<RequestData> {
        let (received, _) = self.socket.recv_from(&mut self.buf)?;

        RequestData::try_from(&self.buf[..received].to_vec())
    }

    /// Address the data socket is bound to
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr().map_err(anyhow::Error::msg)
    }
}
