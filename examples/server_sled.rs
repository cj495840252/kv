use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse, Service, ServiceInner, SledDb};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let path = tempdir().unwrap();
    let store = SledDb::new(path);
    let service: Service<SledDb> = ServiceInner::new(store).fn_before_send(|res|{
        match res.message.as_ref() {
            "" => res.message = "altered. Original message is empty".into(),
            s => res.message = format!("altered:{}",s),
        }
    }).into();
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let svc = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                info!("Got a new command: {:?}", cmd);
                let resp = svc.execute(cmd).next().await.unwrap();
                stream.send(resp.as_ref().to_owned()).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}