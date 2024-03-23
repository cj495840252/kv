use anyhow::Result;
use kv::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};
use tokio::net::TcpListener;
use tracing::info;



#[tokio::main]
async fn main() -> Result<()>{
    tracing_subscriber::fmt::init();

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");
    let ca_cert = include_str!("../fixtures/ca.cert");
    let acceptor = TlsServerAcceptor::new(server_cert, server_key, Some(ca_cert))?;
    
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    let service: Service = ServiceInner::new(MemTable::new()).into();
    info!("Start listening: {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        let stream = acceptor.accept(stream).await?;
        info!("Client {:?} connect", addr);
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move{
            stream.process().await
        });
    }
}