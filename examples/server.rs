use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse, MemTable, Service, ServiceInner};
use tokio::net::TcpListener;
use tracing::info;
use kv::network::tls::TlsServerAcceptor;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let store = MemTable::new();
    let service: Service = ServiceInner::new(store).into();
    let addr = "127.0.0.1:9527";

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");
    let acceptor = TlsServerAcceptor::new(server_cert, server_key, None)?;


    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        let stream = acceptor.accept(stream).await?;
        
        info!("Client {:?} connected", addr);
        let svc = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                info!("Got a new command: {:?}", cmd);
                let mut resp = svc.execute(cmd);
                let data = resp.next().await.unwrap();
                stream.send(data.as_ref().to_owned()).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
    
}