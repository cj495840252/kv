use std::marker::PhantomData;
use futures::{future, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use yamux::{Config, ConnectionError, Control};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, FuturesAsyncReadCompatExt};

/// Yamux控制结构
pub struct YamuxCtrl<S>{
    /// yamux control, 用于创建新的stream
    ctrl: Control,
    _conn: PhantomData<S>
}

impl <S> YamuxCtrl<S> 
where
    S: Send + AsyncRead + AsyncWrite + Unpin + 'static
{
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: futures::Future<Output = Result<(), ConnectionError>> + Send + 'static
    {
        Self::new(stream, config, false, f)
    }


    pub fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self 
    where 
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: futures::Future<Output = Result<(), ConnectionError>> + Send + 'static
    {
        let mode = if is_client {
            yamux::Mode::Client
        }else {
            yamux::Mode::Server
        };
        
        // 创建config
        let mut config = config.unwrap_or_else(Config::default);

        // 这意味着每次从流中读取数据时，都会发送一个窗口更新帧，以增加接收窗口的大小。这样可以确保流总是有足够的空间来接收新的数据。
        config.set_window_update_mode(yamux::WindowUpdateMode::OnRead);

        // 创建config, yamux::Stream 使用的是futures的trait 所以需要compat()到tokio的trait
        let conn = yamux::Connection::new(stream.compat(),  config, mode);
        
        // 创建yamux ctrl
        let ctrl = conn.control();
        
        // pull 所有stream下的数据
        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));
        Self { ctrl, _conn: PhantomData::default() }
    }

    pub async fn open_stream(&mut self) -> Result<Compat<yamux::Stream>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(stream.compat())
    }
}

#[cfg(test)]
mod tests{
    use std::net::SocketAddr;

    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;

    use crate::{tests::assert_res_ok, tls_utils::{tls_acceptor, tls_connector}, CommandRequest, KvError, MemTable, ProstClientStream, ProstServerStream, Service, ServiceInner, Storage, TlsServerAcceptor};
    use anyhow::Result;
    use super::*;
    use tokio_util::compat::FuturesAsyncReadCompatExt;


    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()>{
        // 创建使用了TLS的yamux server
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;

        let connector = tls_connector(false)?;

        let stream = TcpStream::connect(addr).await?;
        let stream = connector.connect(stream).await?;

        // 创建使用了tls的yamux客户端x
        let mut ctrl = YamuxCtrl::new_client(stream, None);
        // 打开一个stream
        let stream1 = ctrl.open_stream().await?;

        // 封装成ProstClientStream
        let mut client = ProstClientStream::new(stream1);
        
        let cmd = CommandRequest::new_hset("table", "key", "value".into());
        client.execute(cmd).await?;

        let cmd = CommandRequest::new_hget("table", "key");
        let res = client.execute(cmd).await?;
        println!("{:?}", res);
        assert_res_ok(&res, &["value".into()], &[]);



        Ok(())
    }

    pub async fn start_server_with<Store>(
        addr: &str, tls: TlsServerAcceptor, store: Store, 
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let service: Service = ServiceInner::new(store).into();
        
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        println!("Client connect success: {:?}", addr);
                        match tls.accept(stream).await {
                            Ok(stream) => f(stream, service.clone()),
                            Err(e) => tracing::warn!("Failed to process TLS: {:?}", e),
                        }
                    },
                    Err(e) => tracing::warn!("Failed to process TCP: {:?}", e)

                }
            }
        });
        Ok(addr)

    }

    pub async fn start_yamux_server<Store>(addr: &str, tls: TlsServerAcceptor, store: Store) -> Result<SocketAddr, KvError> 
    where 
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let f = |stream, service: Service|{
            YamuxCtrl::new_server(stream, None, move |s|{
                let svc = service.clone();
                async move {
                    let stream = ProstServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };
        let addr = start_server_with(addr, tls, store, f).await?;
        Ok(addr)
    }


}