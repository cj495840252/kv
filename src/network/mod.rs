pub mod frame;
pub mod tls;
pub mod multiplex;
pub mod stream_result;
pub use frame::*;
use futures::{ FutureExt, Sink, SinkExt, Stream, StreamExt};
pub use tls::*;
pub use multiplex::*;

use std::{pin::Pin, task::Poll};
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;
use crate::{CommandRequest, CommandResponse, KvError, Service};

use self::stream_result::StreamResult;


macro_rules! ready {
    ($e:expr $(,)?) => {
        match $e {
            std::task::Poll::Pending => return std::task::Poll::Pending,
            std::task::Poll::Ready(t) => t,
        }
    };
}


pub struct ProstServerStream<S>{
    // inner: S,
    inner: ProstStream<S, CommandRequest, CommandResponse>,
    service: Service,
}

pub struct ProstClientStream<S>{
    inner: ProstStream<S, CommandResponse, CommandRequest>,
}


impl <S> ProstServerStream<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send
{
    pub fn new(stream: S, service: Service) -> Self
    {
        Self { inner: ProstStream::new(stream), service }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        let stream = &mut self.inner;
        while let Some(Ok(cmd)) = stream.next().await {
            info!("Got a new command:{:?}", cmd);
            let mut res = self.service.execute(cmd);
            let data = res.next().await.unwrap();
            stream.send(&data).await?;
        };
        Ok(())
    }

    // async fn send(&mut self, msg: CommandResponse) -> Result<(), KvError>{
    //     let mut buf = BytesMut::new();
    //     msg.encode_frame(&mut buf)?;
    //     let encoded = buf.freeze();
    //     self.inner.write_all(&encoded[..]).await?;
    //     Ok(())

    // }
    // async fn recv(&mut self) -> Result<CommandRequest, KvError>{
    //     let mut buf = BytesMut::new();
    //     let stream = &mut self.inner;
    //     read_frame(stream, &mut buf).await?;
    //     CommandRequest::decode_frame(&mut buf)
    // }
}

impl <S> ProstClientStream<S> 
where 
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    pub fn new(stream: S) -> Self{
        Self { inner: ProstStream::new(stream) }
    }

    pub async fn execute(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError>{
        let stream = &mut self.inner;
        stream.send(&cmd).await?;
        match stream.next().await {
            Some(v) => v,
            None => Err(KvError::Internal("Didn't get any response".to_string()))
        }
    }

    pub async fn execute_unary(&mut self, cmd: &CommandRequest) -> Result<CommandResponse, KvError>{
        let stream = &mut self.inner;
        stream.send(cmd).await?;
        let res = stream.next().await;
        match res {
            Some(v) => v,
            None => Err(KvError::Internal("Didn't get any response".to_string()))
        }
    }

    pub async fn execute_streaming(self, cmd: &CommandRequest) -> Result<StreamResult, KvError>{
        let mut stream = self.inner;
        info!("Sending command: {:?}", cmd);
        stream.send(cmd).await?;
        stream.close().await?;

        StreamResult::new(stream).await
    }
}

/// 处理KvServer prost frame 的stream
/// 读数据，Steam返回In，写数据Sink返回Out
pub struct ProstStream<S, In, Out>{
    // inner stream
    stream: S,
    // write buffer
    wbuf: BytesMut,

    //写了多少字节
    written: usize,

    // read buffer
    rbuf: BytesMut,
    // 类型占位符
    _in: std::marker::PhantomData<In>,
    _out: std::marker::PhantomData<Out>,
}

impl <S, In, Out> ProstStream<S, In, Out>
where 
    S: AsyncRead + AsyncWrite + Unpin + Send ,
{
    pub fn new(stream: S) -> Self{
        Self {
            stream,
            wbuf: BytesMut::new(),
            written: 0,
            rbuf: BytesMut::new(),
            _in: std::marker::PhantomData,
            _out: std::marker::PhantomData,
        }
    }
    
}


// 如果之前的stream是Unpin的，那么这个stream也是Unpin的
impl <S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}


impl <S, In, Out> Stream for ProstStream<S, In, Out>
where 
    S: AsyncRead + AsyncWrite + Unpin + Send,
    // 输出的数据要求是本来是CommandRequest的, 
    // 但是经过FrameCoder decode返回一个Message, CommandRequest是一个Message
    In: Unpin + Send +FrameCoder,
    // 输出的数据要求是本来是CommandResponse的,
    // 但是经过FrameCoder encode(encode 是append模式，freeze返回Bytes), 返回一个Bytes
    Out: Unpin + Send ,
{
    /// 调用next时的返回值
    type Item = Result<In, KvError>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        // 上一次调用结束后rbuf应该是空的
        assert!(self.rbuf.is_empty());
        // 从rbuf中分离出rest(摆脱对self的引用)
        let mut rest = self.rbuf.split_off(0);

        // 使用read_frame读取数据
        let fut = read_frame(&mut self.stream, &mut rest);
        ready!(Box::pin(fut).poll_unpin(cx))?;
        
        // 拿到一个frame后，把buffer合并回去
        self.rbuf.unsplit(rest);
        
        // 调用decode_frame解码
        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}


impl <S, In, Out> Sink<&Out> for ProstStream<S, In, Out>
where 
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send ,
    Out: Unpin + Send + FrameCoder,
{
    /// 如果发出出错，返回错误
    type Error = KvError;

    fn poll_ready(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// 将CommandRequest encode成frame 放入写缓冲区
    fn start_send(self: std::pin::Pin<&mut Self>, item: &Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.wbuf)?;
        Ok(())
    }

    /// 每次写入written
    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        //循环写入stream中
        while this.written != this.wbuf.len(){
            // poll_write是AsyncWrite的trait的方法
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &mut this.wbuf[this.written..]))?;
            this.written += n;
            
        }
        //写完后清空buffer
        this.wbuf.clear();
        this.written = 0;

        // 调用stream的poll_flush确保写入
        ready!(Pin::new(&mut this.stream).poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        // 调用stream的poll_close确保写入
        ready!(self.as_mut().poll_flush(cx))?;

        // 调用stream的poll_shutdown确保关闭
        ready!(Pin::new(&mut self.stream).poll_shutdown(cx))?;
        Poll::Ready(Ok(()))

    }
    
}

#[cfg(test)]
mod tests{
    use std::net::SocketAddr;
    use bytes::{Bytes, BytesMut};
    use futures::{SinkExt, StreamExt as _};
    use tokio::net::{TcpListener, TcpStream};
    use anyhow::Result;
    use crate::{ CommandRequest, MemTable, ProstStream, Service, ServiceInner, Value};
    use super::{ProstServerStream, ProstClientStream};
    use crate::service::tests::assert_res_ok;
    use super::utils::DummyStream;

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
        assert_res_ok(&res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hset("table", "key", "value2".into());
        let res = client.execute(cmd).await?;
        // 第二次set应该返回第一次的结果
        assert_res_ok(&res, &["value1".into()], &[]);


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

        assert_res_ok(&res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("table", "key");
        let res = client.execute(cmd).await?;
        assert_res_ok(&res, &[v.into()], &[]);
        Ok(())

    }

    #[tokio::test]
    async fn prost_stream_should_work() -> anyhow::Result<()>{
        let buf = BytesMut::new();
        let stream = DummyStream{buf};
        let mut stream: ProstStream<_, CommandRequest, CommandRequest> = ProstStream::new(stream);
        let cmd = CommandRequest::new_hget("table", "key");
        stream.send(&cmd).await?;

        if let Some(Ok(res)) = stream.next().await {
            assert_eq!(res, cmd);
        }else {
            assert!(false, "Should get a CommandRequest")
        }

        Ok(())
    }
}



#[cfg(test)]
pub mod utils{
    use super::*;

    pub fn is_compressed(data: &[u8]) -> bool{
        if let &[v] = &data[..1] {
            v >> 7 == 1
        }else {
            false
        }
    }

    pub struct DummyStream{
        pub buf: BytesMut
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// 实现一个简单的AsyncWrite只需要把数据写入到Bytes中，其他的直接返回OK
    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.get_mut().buf.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_frame_should_work(){
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("table", "key");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream{buf};

        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1)
    }

    
}