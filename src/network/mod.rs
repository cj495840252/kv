mod frame;
use bytes::BytesMut;
pub use frame::FrameCoder;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError, Service};

use self::frame::read_frame;

pub struct ProstServerStream<S>{
    inner: S,
    service: Service,
}

pub struct ProstClientStream<S>{
    inner: S,
}


impl <S> ProstServerStream<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send
{
    pub fn new(stream: S, service: Service) -> Self{
        Self { inner: stream, service }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Ok(cmd) = self.recv().await {
            info!("Got a new command:{:?}", cmd);
            let res = self.service.execute(cmd);
            self.send(res).await?;
        };
        Ok(())
    }

    async fn send(&mut self, msg: CommandResponse) -> Result<(), KvError>{
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())

    }
    async fn recv(&mut self) -> Result<CommandRequest, KvError>{
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        CommandRequest::decode_frame(&mut buf)
    }
}

impl <S> ProstClientStream<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send
{
    pub fn new(stream: S) -> Self{
        Self { inner: stream }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError>{
        self.send(cmd).await?;
        Ok(self.recv().await?)
    }

    pub async fn send(&mut self, msg: CommandRequest)-> Result<(), KvError>{
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        self.inner.write_all(&buf).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<CommandResponse, KvError> {
        let mut buf = BytesMut::new();

        read_frame(&mut self.inner, &mut buf).await?;

        CommandResponse::decode_frame(&mut buf)
    }
}

#[cfg(test)]
mod tests{
    use std::net::SocketAddr;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};
    use anyhow::Result;
    use crate::{ CommandRequest, MemTable, Service, ServiceInner, Value};
    use super::{ProstServerStream, ProstClientStream};
    use crate::service::tests::assert_res_ok;

    async fn start_server() -> Result<SocketAddr>{
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
           loop {
               let (stream, _) = listener.accept().await.unwrap();
               let service: Service = ServiceInner::new(MemTable::new()).into();
               let server = ProstServerStream::new(stream, service);
               tokio::spawn(server.process());
           } 
        });
        Ok(addr)
    }

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()>{
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);

        let cmd = CommandRequest::new_hset("table", "key", "value1".into());
        let res = client.execute(cmd).await?;
        // 第一次set应该返回None
        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hset("table", "key", "value2".into());
        let res = client.execute(cmd).await?;
        // 第二次set应该返回第一次的结果
        assert_res_ok(res, &["value1".into()], &[]);


        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work()-> anyhow::Result<()>{
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);
        let v: Value = Bytes::from(vec![0u8;16348]).into();
        let cmd = CommandRequest::new_hset("table", "key", v.clone());
        let res = client.execute(cmd).await?;

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("table", "key");
        let res = client.execute(cmd).await?;
        assert_res_ok(res, &[v.into()], &[]);
        Ok(())

    }
}