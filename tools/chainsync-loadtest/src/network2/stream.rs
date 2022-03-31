use bytes::{BytesMut};
use bytesize::{MIB};
use borsh::{BorshDeserialize, BorshSerialize};
use tokio::net;
use tokio::io;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex};
use anyhow::{anyhow};

use crate::peer_manager::types::{PeerMessage};
use crate::concurrency::{Ctx};

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;

pub struct Stream {
    reader : AsyncMutex<io::BufReader<io::ReadHalf<net::TcpStream>>>,
    writer : AsyncMutex<io::BufWriter<io::WriteHalf<net::TcpStream>>>,
}

impl Stream {
    pub fn new(stream: net::TcpStream) -> Self {
        let (reader,writer) = io::split(stream);
        Self{
            reader: AsyncMutex::new(io::BufReader::new(reader)),
            writer: AsyncMutex::new(io::BufWriter::new(writer)),
        }
    }
    
    pub async fn read(&self, ctx: &Ctx) -> anyhow::Result<PeerMessage> {
        let mut reader = ctx.wrap(self.reader.lock()).await?;
        let n = ctx.wrap(reader.read_u32_le()).await?? as usize;
		// TODO: if this check fails, the stream is broken, because we read some bytes already.
		if n>NETWORK_MESSAGE_MAX_SIZE_BYTES { return Err(anyhow!("message size too large")); }
        let mut buf = BytesMut::new();
	    buf.resize(n,0);	
        ctx.wrap(reader.read_exact(&mut buf[..])).await??;
        Ok(PeerMessage::try_from_slice(&buf[..])?)
    }
    
    pub async fn write(&self, ctx: &Ctx, msg: &PeerMessage) -> anyhow::Result<()> {
        let msg = msg.try_to_vec()?;
        let mut writer = ctx.wrap(self.writer.lock()).await?; 
        // TODO: If writing fails in the middle, then the stream is broken.
        ctx.wrap(writer.write_u32_le(msg.len() as u32)).await??;
        ctx.wrap(writer.write_all(&msg[..])).await??;
        ctx.wrap(writer.flush()).await??;
        Ok(())
    } 
}
