pub mod request;

use std::{
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc, Mutex, MutexGuard,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};
use log::{debug, error, warn};

use crate::conn::request::{Request, RequestData};

/// Address both sockets are bound to
const BIND_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
/// Control socket port, shared by every peer
pub const CONTROL_SOCKET_PORT: u16 = 5000;
/// Data socket port, shared by every peer
pub const DATA_SOCKET_PORT: u16 = 5001;
/// Maximum UDP datagram payload size
const DATAGRAM_SIZE: usize = 65_535;
/// Data socket read timeout, doubles as the peer acknowledgement deadline
const DATA_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Attempts to deliver a request before giving up on the peer
const MAX_SEND_ATTEMPTS: u8 = 3;

/// Shared peer connection reference
pub type PeerConnRef = Arc<PeerConn>;

/// Centralized socket connections (data and control)
pub struct PeerConn {
    /// Control socket with signalling requests
    control_socket: Arc<UdpSocket>,
    /// Data socket with file content and file tree transfers
    data_socket: Mutex<UdpSocket>,
    /// Requests read from the control socket
    requests: Mutex<Receiver<(SocketAddr, Result<RequestData>)>>,
    /// Acknowledgements read from the control socket
    acks: Mutex<Receiver<SocketAddr>>,
    /// How long a peer has to acknowledge a request
    ack_timeout: Duration,
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
        Self::with_ack_timeout(control_socket, data_socket, DATA_READ_TIMEOUT)
    }

    fn with_ack_timeout(
        control_socket: UdpSocket,
        data_socket: UdpSocket,
        ack_timeout: Duration,
    ) -> Result<Self> {
        data_socket.set_read_timeout(Some(ack_timeout))?;

        let control_socket = Arc::new(control_socket);
        let (request_sender, requests) = channel();
        let (ack_sender, acks) = channel();
        let reader_socket = control_socket.clone();
        thread::Builder::new()
            .name("PeerConnControlReaderThread".into())
            .spawn(move || read_control(&reader_socket, &request_sender, &ack_sender))?;

        Ok(Self {
            control_socket,
            data_socket: Mutex::new(data_socket),
            requests: Mutex::new(requests),
            acks: Mutex::new(acks),
            ack_timeout,
        })
    }

    /// Receives the next control request, already acknowledged to its sender
    pub fn recv_control(&self) -> Result<(SocketAddr, Result<RequestData>)> {
        Ok(self
            .requests
            .lock()
            .map_err(|e| anyhow!("Could not acquire control request lock: {e}"))?
            .recv()?)
    }

    /// Sends a request to the peer control socket
    pub fn send_control(&self, data: RequestData, peer: IpAddr) -> Result<()> {
        self.reply_control(data, SocketAddr::new(peer, CONTROL_SOCKET_PORT))
    }

    /// Sends a request back to the exact address a control request came from
    pub fn reply_control(&self, data: RequestData, addr: SocketAddr) -> Result<()> {
        let bytes: Box<[u8]> = Request { data }.into();
        let acks = self
            .acks
            .lock()
            .map_err(|e| anyhow!("Could not acquire acknowledgement lock: {e}"))?;
        while acks.try_recv().is_ok() {}

        for attempt in 1..=MAX_SEND_ATTEMPTS {
            debug!(
                "Sending control request to {addr} ({} bytes, attempt {attempt}): {:?}",
                bytes.len(),
                String::from_utf8(bytes.to_vec())
            );
            self.control_socket.send_to(&bytes, addr)?;

            if wait_ack(&acks, addr, self.ack_timeout) {
                return Ok(());
            }
            warn!("Peer {addr} did not acknowledge control request");
        }

        bail!("Peer {addr} did not acknowledge request after {MAX_SEND_ATTEMPTS} attempts")
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
    pub fn send(&mut self, data: RequestData, peer: IpAddr) -> Result<()> {
        let addr = SocketAddr::new(peer, DATA_SOCKET_PORT);
        let bytes: Box<[u8]> = Request { data }.into();

        for attempt in 1..=MAX_SEND_ATTEMPTS {
            debug!(
                "Sending data request to {addr} ({} bytes, attempt {attempt})",
                bytes.len()
            );
            self.socket.send_to(&bytes, addr)?;

            if self.recv_ack()? {
                return Ok(());
            }
            warn!("Peer {addr} did not acknowledge data request");
        }

        bail!("Peer {addr} did not acknowledge request after {MAX_SEND_ATTEMPTS} attempts")
    }

    /// Receives the next request from the data socket and acknowledges it
    pub fn recv(&mut self) -> Result<RequestData> {
        loop {
            let (received, src) = self.socket.recv_from(&mut self.buf)?;
            let data = RequestData::try_from(&self.buf[..received].to_vec())?;
            if matches!(data, RequestData::Acknowledgement) {
                debug!("Discarding stale acknowledgement from {src}");
                continue;
            }

            acknowledge(&self.socket, src)?;

            return Ok(data);
        }
    }

    /// Address the data socket is bound to
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr().map_err(anyhow::Error::msg)
    }

    /// Waits for the peer acknowledgement, returning `false` when the read times out
    fn recv_ack(&mut self) -> Result<bool> {
        match self.socket.recv_from(&mut self.buf) {
            Ok((received, _)) => Ok(matches!(
                RequestData::try_from(&self.buf[..received].to_vec()),
                Ok(RequestData::Acknowledgement)
            )),
            Err(e) if is_timeout(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Reads the control socket, acknowledging requests and dispatching them to [`PeerConn`]
fn read_control(
    socket: &UdpSocket,
    requests: &Sender<(SocketAddr, Result<RequestData>)>,
    acks: &Sender<SocketAddr>,
) {
    let mut buf = vec![0; DATAGRAM_SIZE];
    loop {
        let (received, src) = match socket.recv_from(&mut buf) {
            Ok(read) => read,
            Err(e) => {
                error!("Control socket read failure: {e}");
                return;
            }
        };

        let dispatched = match RequestData::try_from(&buf[..received].to_vec()) {
            Ok(RequestData::Acknowledgement) => acks.send(src).is_ok(),
            Ok(data) => {
                if let Err(e) = acknowledge(socket, src) {
                    error!("Could not acknowledge request from {src}: {e}");
                }
                requests.send((src, Ok(data))).is_ok()
            }
            Err(e) => requests.send((src, Err(e))).is_ok(),
        };
        if !dispatched {
            return;
        }
    }
}

/// Waits until the peer acknowledges within `timeout`
fn wait_ack(acks: &Receiver<SocketAddr>, addr: SocketAddr, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match acks.recv_timeout(remaining) {
            Ok(src) if src.ip() == addr.ip() => return true,
            Ok(src) => debug!("Discarding acknowledgement from unexpected peer {src}"),
            Err(_) => break,
        }
    }

    false
}

/// Answers a received request with an acknowledgement
fn acknowledge(socket: &UdpSocket, addr: SocketAddr) -> Result<()> {
    let bytes: Box<[u8]> = Request {
        data: RequestData::Acknowledgement,
    }
    .into();
    socket.send_to(&bytes, addr)?;

    Ok(())
}

/// Returns whether the socket error was caused by a read timeout
fn is_timeout(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::utils::test_net;

    /// Acknowledgement deadline used by tests, keeps retries fast
    const TEST_ACK_TIMEOUT: Duration = Duration::from_millis(100);
    /// Read timeout of the peer sockets simulating another host
    const PEER_READ_TIMEOUT: Duration = Duration::from_millis(500);
    /// Delivery attempts the protocol requires before failing
    const EXPECTED_ATTEMPTS: u8 = 3;

    fn test_conn() -> PeerConn {
        PeerConn::with_ack_timeout(
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
            TEST_ACK_TIMEOUT,
        )
        .unwrap()
    }

    fn peer_socket(port: u16) -> UdpSocket {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).unwrap();
        socket.set_read_timeout(Some(PEER_READ_TIMEOUT)).unwrap();

        socket
    }

    fn send(socket: &UdpSocket, data: RequestData, addr: SocketAddr) {
        let bytes: Box<[u8]> = Request { data }.into();
        socket.send_to(&bytes, addr).unwrap();
    }

    /// Reads the next datagram, returning `None` when the read times out
    fn recv(socket: &UdpSocket) -> Option<(RequestData, SocketAddr)> {
        let mut buf = [0; 2048];
        match socket.recv_from(&mut buf) {
            Ok((received, src)) => Some((
                RequestData::try_from(&buf[..received].to_vec()).unwrap(),
                src,
            )),
            Err(e) if is_timeout(&e) => None,
            Err(e) => panic!("Unexpected socket error: {e}"),
        }
    }

    /// Answers the next request with an acknowledgement and returns it
    fn ack_next_request(socket: &UdpSocket) -> RequestData {
        let (data, src) = recv(socket).expect("no request received");
        send(socket, RequestData::Acknowledgement, src);

        data
    }

    /// Test received control request is acknowledged to its sender
    #[test]
    fn test_control_request_is_acknowledged() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let expected = RequestData::NewFile {
            path: PathBuf::from("new.txt"),
        };

        send(
            &peer,
            expected.clone(),
            conn.control_socket.local_addr().unwrap(),
        );

        let (src, data) = conn.recv_control().unwrap();
        assert_eq!(src, peer.local_addr().unwrap());
        assert_eq!(data.unwrap(), expected);
        assert_eq!(recv(&peer).unwrap().0, RequestData::Acknowledgement);
    }

    /// Test malformed control request is reported without acknowledgement
    #[test]
    fn test_malformed_control_request_is_not_acknowledged() {
        let conn = test_conn();
        let peer = peer_socket(0);

        peer.send_to(b"NOPE", conn.control_socket.local_addr().unwrap())
            .unwrap();

        let (_, data) = conn.recv_control().unwrap();
        assert!(data.is_err());
        assert!(recv(&peer).is_none());
    }

    /// Test acknowledgement is consumed instead of dispatched as a request
    #[test]
    fn test_control_acknowledgement_is_not_dispatched() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let conn_addr = conn.control_socket.local_addr().unwrap();

        send(&peer, RequestData::Acknowledgement, conn_addr);
        send(&peer, RequestData::GetFileTree, conn_addr);

        let (_, data) = conn.recv_control().unwrap();
        assert_eq!(data.unwrap(), RequestData::GetFileTree);
        assert_eq!(recv(&peer).unwrap().0, RequestData::Acknowledgement);
        assert!(recv(&peer).is_none());
    }

    /// Test control request is sent once when the peer acknowledges it
    #[test]
    fn test_reply_control_waits_for_acknowledgement() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let peer_addr = peer.local_addr().unwrap();
        let responder = thread::spawn(move || {
            let data = ack_next_request(&peer);
            assert!(recv(&peer).is_none(), "request was retried after ack");

            data
        });

        conn.reply_control(RequestData::GetFileTree, peer_addr)
            .unwrap();

        assert_eq!(responder.join().unwrap(), RequestData::GetFileTree);
    }

    /// Test control request is retried until the peer acknowledges it
    #[test]
    fn test_reply_control_retries_until_acknowledged() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let peer_addr = peer.local_addr().unwrap();
        let responder = thread::spawn(move || {
            for attempt in 1..EXPECTED_ATTEMPTS {
                assert!(recv(&peer).is_some(), "request not retried ({attempt})");
            }

            ack_next_request(&peer)
        });

        conn.reply_control(RequestData::GetFileTree, peer_addr)
            .unwrap();

        assert_eq!(responder.join().unwrap(), RequestData::GetFileTree);
    }

    /// Test control request fails after exhausting every delivery attempt
    #[test]
    fn test_reply_control_fails_without_acknowledgement() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let peer_addr = peer.local_addr().unwrap();

        assert!(
            conn.reply_control(RequestData::GetFileTree, peer_addr)
                .is_err()
        );

        let mut attempts = 0;
        while recv(&peer).is_some() {
            attempts += 1;
        }
        assert_eq!(attempts, EXPECTED_ATTEMPTS);
    }

    /// Test control request is addressed to the peer control port
    #[test]
    fn test_send_control_targets_control_port() {
        let _port_guard = test_net::control_port_guard();
        let conn = test_conn();
        let peer = peer_socket(CONTROL_SOCKET_PORT);
        let responder = thread::spawn(move || ack_next_request(&peer));

        conn.send_control(RequestData::GetFileTree, Ipv4Addr::LOCALHOST.into())
            .unwrap();

        assert_eq!(responder.join().unwrap(), RequestData::GetFileTree);
    }

    /// Test received data request is acknowledged to its sender
    #[test]
    fn test_data_request_is_acknowledged() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let data_addr = conn.data_channel().unwrap().local_addr().unwrap();

        send(&peer, RequestData::EndOfTree, data_addr);

        assert_eq!(
            conn.data_channel().unwrap().recv().unwrap(),
            RequestData::EndOfTree
        );
        assert_eq!(recv(&peer).unwrap().0, RequestData::Acknowledgement);
    }

    /// Test stale acknowledgement is skipped while waiting for a data request
    #[test]
    fn test_data_recv_skips_stale_acknowledgement() {
        let conn = test_conn();
        let peer = peer_socket(0);
        let data_addr = conn.data_channel().unwrap().local_addr().unwrap();

        send(&peer, RequestData::Acknowledgement, data_addr);
        send(&peer, RequestData::EndOfTree, data_addr);

        assert_eq!(
            conn.data_channel().unwrap().recv().unwrap(),
            RequestData::EndOfTree
        );
    }

    /// Test data request is sent once when the peer acknowledges it
    #[test]
    fn test_data_send_waits_for_acknowledgement() {
        let _port_guard = test_net::data_port_guard();
        let conn = test_conn();
        let peer = peer_socket(DATA_SOCKET_PORT);
        let responder = thread::spawn(move || {
            let data = ack_next_request(&peer);
            assert!(recv(&peer).is_none(), "request was retried after ack");

            data
        });

        conn.data_channel()
            .unwrap()
            .send(RequestData::EndOfTree, Ipv4Addr::LOCALHOST.into())
            .unwrap();

        assert_eq!(responder.join().unwrap(), RequestData::EndOfTree);
    }

    /// Test data request is retried until the peer acknowledges it
    #[test]
    fn test_data_send_retries_until_acknowledged() {
        let _port_guard = test_net::data_port_guard();
        let conn = test_conn();
        let peer = peer_socket(DATA_SOCKET_PORT);
        let responder = thread::spawn(move || {
            for attempt in 1..EXPECTED_ATTEMPTS {
                assert!(recv(&peer).is_some(), "request not retried ({attempt})");
            }

            ack_next_request(&peer)
        });

        conn.data_channel()
            .unwrap()
            .send(RequestData::EndOfTree, Ipv4Addr::LOCALHOST.into())
            .unwrap();

        assert_eq!(responder.join().unwrap(), RequestData::EndOfTree);
    }

    /// Test data request fails after exhausting every delivery attempt
    #[test]
    fn test_data_send_fails_without_acknowledgement() {
        let _port_guard = test_net::data_port_guard();
        let conn = test_conn();
        let peer = peer_socket(DATA_SOCKET_PORT);

        assert!(
            conn.data_channel()
                .unwrap()
                .send(RequestData::EndOfTree, Ipv4Addr::LOCALHOST.into())
                .is_err()
        );

        let mut attempts = 0;
        while recv(&peer).is_some() {
            attempts += 1;
        }
        assert_eq!(attempts, EXPECTED_ATTEMPTS);
    }
}
