use std::{net::IpAddr, path::PathBuf, str::FromStr};

use anyhow::bail;
use log::debug;

/// Application request
pub struct Request {
    /// Request data content
    pub data: RequestData,
}

impl From<Request> for Box<[u8]> {
    fn from(val: Request) -> Self {
        let verb: RequestVerb = (&val.data).into();
        let mut bytes: Vec<u8> = verb.into();
        bytes.append(&mut val.data.into());

        bytes.into_boxed_slice()
    }
}

impl TryFrom<&Vec<u8>> for Request {
    type Error = anyhow::Error;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            data: RequestData::try_from(value)?,
        })
    }
}

/// Request data content
#[derive(Clone, Debug, PartialEq)]
pub enum RequestData {
    /// Add new peer
    NewPeer {
        /// Peer address
        addr: IpAddr,
    },
    /// Remove peer
    RemovePeer {
        /// Peer address
        addr: IpAddr,
    },
    /// File bytes chunk
    FileContent {
        /// File path
        path: PathBuf,
        /// Chunk index
        part: u32,
        /// Content bytes
        content: Vec<u8>,
    },
    /// File stream end
    EndOfFile {
        /// Content checksum
        sha256: String,
    },
    /// File tree end
    EndOfTree,
    /// Request file content by path
    GetFileContent {
        /// File path
        path: PathBuf,
    },
    /// List all files
    GetFileTree,
    /// Send file path and content hash
    ListFiles {
        /// File content hash
        sha256: String,
        /// File path
        path: PathBuf,
    },
    /// File moved
    MovedFile {
        /// Source path
        from: PathBuf,
        /// Target path
        to: PathBuf,
    },
    /// File created
    NewFile {
        /// File path
        path: PathBuf,
    },
    /// File removed
    RemoveFile {
        /// File path
        path: PathBuf,
    },
}

impl From<&RequestData> for RequestVerb {
    fn from(val: &RequestData) -> Self {
        match val {
            RequestData::NewPeer { .. } => RequestVerb::ADDP,
            RequestData::FileContent { .. } => RequestVerb::CAT,
            RequestData::EndOfFile { .. } => RequestVerb::EOF,
            RequestData::EndOfTree => RequestVerb::LSE,
            RequestData::GetFileContent { .. } => RequestVerb::GET,
            RequestData::GetFileTree => RequestVerb::TREE,
            RequestData::ListFiles { .. } => RequestVerb::LS,
            RequestData::MovedFile { .. } => RequestVerb::MV,
            RequestData::NewFile { .. } => RequestVerb::NEW,
            RequestData::RemoveFile { .. } => RequestVerb::RM,
            RequestData::RemovePeer { .. } => RequestVerb::RMP,
        }
    }
}

impl From<RequestData> for Vec<u8> {
    fn from(val: RequestData) -> Self {
        match val {
            RequestData::NewPeer { addr } | RequestData::RemovePeer { addr } => {
                addr.to_string().as_bytes().into()
            }
            RequestData::FileContent {
                path,
                part,
                mut content,
            } => {
                let mut bytes: Vec<u8> =
                    format!("{} {part:0>6} ", path.display()).as_bytes().into();
                bytes.append(&mut content);

                bytes
            }
            RequestData::EndOfFile { sha256 } => sha256.as_bytes().into(),
            RequestData::GetFileContent { path }
            | RequestData::NewFile { path }
            | RequestData::RemoveFile { path } => format!("{}", path.display()).as_bytes().into(),
            RequestData::MovedFile { from, to } => format!("{} {}", from.display(), to.display())
                .as_bytes()
                .into(),
            RequestData::ListFiles { sha256, path } => {
                format!("{sha256} {}", path.display()).as_bytes().into()
            }
            RequestData::EndOfTree | RequestData::GetFileTree => vec![],
        }
    }
}

impl TryFrom<Vec<u8>> for RequestData {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&Vec<u8>> for RequestData {
    type Error = anyhow::Error;

    fn try_from(value: &Vec<u8>) -> Result<Self, Self::Error> {
        debug!(
            "Parsing request ({} bytes): {:?}",
            value.len(),
            String::from_utf8(value.to_vec())
        );
        let (verb_bytes, request_data) = value.split_at(4);

        let verb_bytes_vec: Vec<u8> = verb_bytes.into();
        let verb: RequestVerb = verb_bytes_vec.try_into()?;

        match verb {
            RequestVerb::ADDP => {
                let addr_raw = String::from_utf8(request_data.to_vec())?;
                let addr = IpAddr::from_str(&addr_raw)?;

                Ok(Self::NewPeer { addr })
            }
            RequestVerb::CAT => {
                let params: Vec<&[u8]> = request_data.splitn(3, |b| *b == b' ').collect();
                if params.len() != 3 {
                    bail!("Invalid param size: {}", params.len())
                }

                let path_bytes = params[0].to_vec();
                let path = String::from_utf8(path_bytes)?.into();

                let part_bytes = params[1].to_vec();
                let part = String::from_utf8(part_bytes)?.parse()?;

                Ok(RequestData::FileContent {
                    path,
                    part,
                    content: params[2].to_vec(),
                })
            }
            RequestVerb::EOF => Ok(RequestData::EndOfFile {
                sha256: String::from_utf8(request_data.to_vec())?,
            }),
            RequestVerb::GET => Ok(RequestData::GetFileContent {
                path: String::from_utf8(request_data.to_vec())?.into(),
            }),
            RequestVerb::LS => {
                let params: Vec<&[u8]> = request_data.splitn(2, |b| *b == b' ').collect();
                if params.len() != 2 {
                    bail!("Invalid param size: {}", params.len())
                }

                let hash = String::from_utf8(params[0].to_vec())?;
                if hash.is_empty() {
                    bail!("Empty hash value")
                }
                let path_str = String::from_utf8(params[1].to_vec())?;

                Ok(RequestData::ListFiles {
                    sha256: hash,
                    path: PathBuf::from(path_str),
                })
            }
            RequestVerb::LSE => Ok(RequestData::EndOfTree),
            RequestVerb::MV => {
                let params: Vec<&[u8]> = request_data.splitn(2, |b| *b == b' ').collect();
                if params.len() != 2 {
                    bail!("Invalid param size: {}", params.len())
                }

                let from_str = String::from_utf8(params[0].to_vec())?;
                let to_str = String::from_utf8(params[1].to_vec())?;

                if from_str.is_empty() || to_str.is_empty() {
                    bail!("Empty file path")
                }

                Ok(RequestData::MovedFile {
                    from: from_str.into(),
                    to: to_str.into(),
                })
            }
            RequestVerb::NEW => {
                let path_str = String::from_utf8(request_data.to_vec())?;
                if path_str.is_empty() {
                    bail!("Empty file path")
                }

                Ok(RequestData::NewFile {
                    path: path_str.into(),
                })
            }
            RequestVerb::RM => {
                let path_str = String::from_utf8(request_data.to_vec())?;
                if path_str.is_empty() {
                    bail!("Empty file path")
                }

                Ok(RequestData::RemoveFile {
                    path: path_str.into(),
                })
            }
            RequestVerb::RMP => {
                let addr_raw = String::from_utf8(request_data.to_vec())?;
                let addr = IpAddr::from_str(&addr_raw)?;

                Ok(Self::RemovePeer { addr })
            }
            RequestVerb::TREE => Ok(RequestData::GetFileTree),
        }
    }
}

/// Request verb
#[derive(Debug, PartialEq)]
enum RequestVerb {
    /// Add new peer
    ADDP,
    /// Get file content
    CAT,
    /// End of file stream
    EOF,
    /// Request file content
    GET,
    /// List file content
    LS,
    /// List file content end
    LSE,
    /// File moved
    MV,
    /// New file created
    NEW,
    /// File removed
    RM,
    /// Peer removed
    RMP,
    /// List all files
    TREE,
}

impl From<RequestVerb> for Vec<u8> {
    fn from(val: RequestVerb) -> Self {
        match val {
            RequestVerb::ADDP => b"ADDP",
            RequestVerb::CAT => b"CAT ",
            RequestVerb::EOF => b"EOF ",
            RequestVerb::GET => b"GET ",
            RequestVerb::LS => b"LS  ",
            RequestVerb::LSE => b"LSE ",
            RequestVerb::MV => b"MV  ",
            RequestVerb::NEW => b"NEW ",
            RequestVerb::RM => b"RM  ",
            RequestVerb::RMP => b"RMP ",
            RequestVerb::TREE => b"TREE",
        }
        .into()
    }
}

impl TryFrom<Vec<u8>> for RequestVerb {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match value {
            val if val == b"ADDP" => Ok(RequestVerb::ADDP),
            val if val == b"CAT " => Ok(RequestVerb::CAT),
            val if val == b"EOF " => Ok(RequestVerb::EOF),
            val if val == b"GET " => Ok(RequestVerb::GET),
            val if val == b"LS  " => Ok(RequestVerb::LS),
            val if val == b"LSE " => Ok(RequestVerb::LSE),
            val if val == b"MV  " => Ok(RequestVerb::MV),
            val if val == b"NEW " => Ok(RequestVerb::NEW),
            val if val == b"RM  " => Ok(RequestVerb::RM),
            val if val == b"RMP " => Ok(RequestVerb::RMP),
            val if val == b"TREE" => Ok(RequestVerb::TREE),
            _ => bail!("Invalid verb: {value:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// Test verb conversion into bytes
    #[test]
    fn test_verb_into_bytes() {
        [
            (b"ADDP", RequestVerb::ADDP),
            (b"CAT ", RequestVerb::CAT),
            (b"EOF ", RequestVerb::EOF),
            (b"GET ", RequestVerb::GET),
            (b"MV  ", RequestVerb::MV),
            (b"NEW ", RequestVerb::NEW),
            (b"RM  ", RequestVerb::RM),
        ]
        .into_iter()
        .for_each(|(expected, verb)| assert_eq!(Into::<Vec<u8>>::into(verb), expected.to_vec()));
    }

    /// Test successful verb parse from bytes
    #[test]
    fn test_verb_from_bytes_ok() {
        [
            (RequestVerb::ADDP, b"ADDP"),
            (RequestVerb::CAT, b"CAT "),
            (RequestVerb::EOF, b"EOF "),
            (RequestVerb::GET, b"GET "),
            (RequestVerb::MV, b"MV  "),
            (RequestVerb::NEW, b"NEW "),
            (RequestVerb::RM, b"RM  "),
        ]
        .into_iter()
        .for_each(|(expected, verb)| {
            assert_eq!(expected, RequestVerb::try_from(verb.to_vec()).unwrap())
        });
    }

    /// Test invalid verb parse from bytes
    #[test]
    fn test_verb_from_invalid_bytes() {
        ["RM ", "RM    ", "", "UNKNOWN"]
            .into_iter()
            .for_each(|verb| assert!(RequestVerb::try_from(verb.as_bytes().to_vec()).is_err()));
    }

    /// Test file content request data into bytes
    #[test]
    fn test_data_into_bytes_file_content() {
        let data: Vec<u8> = RequestData::FileContent {
            path: PathBuf::from("test.txt"),
            part: 1,
            content: b"file content\n".to_vec(),
        }
        .into();

        assert_eq!(b"test.txt 000001 file content\n".to_vec(), data);
    }

    /// Test end-of-file request data into bytes
    #[test]
    fn test_data_into_bytes_eof() {
        let data: Vec<u8> = RequestData::EndOfFile {
            sha256: "sha256hash".into(),
        }
        .into();

        assert_eq!(b"sha256hash".to_vec(), data);
    }

    /// Test add peer request data into bytes
    #[test]
    fn test_data_into_bytes_new_peer() {
        let data: Vec<u8> = RequestData::NewPeer {
            addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
        }
        .into();

        assert_eq!(b"127.0.0.1".to_vec(), data);
    }

    /// Test get content request data into bytes
    #[test]
    fn test_data_into_bytes_get_content() {
        let data: Vec<u8> = RequestData::GetFileContent {
            path: PathBuf::from("test.txt"),
        }
        .into();

        assert_eq!(b"test.txt".to_vec(), data);
    }

    /// Test moved file request data into bytes
    #[test]
    fn test_data_into_bytes_moved_file() {
        let data: Vec<u8> = RequestData::MovedFile {
            from: PathBuf::from("old.txt"),
            to: PathBuf::from("new.txt"),
        }
        .into();
        assert_eq!(b"old.txt new.txt".to_vec(), data);

        let data: Vec<u8> = RequestData::MovedFile {
            from: PathBuf::from("old.txt"),
            to: PathBuf::from("subdir/new.txt"),
        }
        .into();
        assert_eq!(b"old.txt subdir/new.txt".to_vec(), data);
    }

    /// Test new file request data into bytes
    #[test]
    fn test_data_into_bytes_new_file() {
        let data: Vec<u8> = RequestData::NewFile {
            path: PathBuf::from("test.txt"),
        }
        .into();
        assert_eq!(b"test.txt".to_vec(), data);
    }

    /// Test remove file request data into bytes
    #[test]
    fn test_data_into_bytes_remove_file() {
        let data: Vec<u8> = RequestData::RemoveFile {
            path: PathBuf::from("test.txt"),
        }
        .into();
        assert_eq!(b"test.txt".to_vec(), data);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_new_peer() {
        let expected = RequestData::NewPeer {
            addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
        };
        let result = RequestData::try_from(b"ADDP127.0.0.1".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_file_content() {
        let expected = RequestData::FileContent {
            path: PathBuf::from("test.txt"),
            part: 1,
            content: b"file content\n".to_vec(),
        };
        let result = RequestData::try_from(b"CAT test.txt 000001 file content\n".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_eof() {
        let expected = RequestData::EndOfFile {
            sha256: "sha256hash".into(),
        };
        let result = RequestData::try_from(b"EOF sha256hash".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_get_content() {
        let expected = RequestData::GetFileContent {
            path: PathBuf::from("test.txt"),
        };
        let result = RequestData::try_from(b"GET test.txt".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_moved_file() {
        let expected = RequestData::MovedFile {
            from: PathBuf::from("old.txt"),
            to: PathBuf::from("new.txt"),
        };
        let result = RequestData::try_from(b"MV  old.txt new.txt".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_new_file() {
        let expected = RequestData::NewFile {
            path: PathBuf::from("test.txt"),
        };
        let result = RequestData::try_from(b"NEW test.txt".to_vec()).unwrap();

        assert_eq!(expected, result);
    }

    /// Test parse request data from bytes
    #[test]
    fn test_data_from_bytes_remove_file() {
        let expected = RequestData::RemoveFile {
            path: PathBuf::from("test.txt"),
        };
        let result = RequestData::try_from(b"RM  test.txt".to_vec()).unwrap();

        assert_eq!(expected, result);
    }
}
