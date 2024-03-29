use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{network::TlsClientConnector, CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ca_cert = include_str!("../fixtures/ca.cert");

    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca_cert))?;


    let addr = "127.0.0.1:9527";
    // 连接服务器
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    // 使用 AsyncProstStream 来处理 TCP Frame
    let mut client =
        AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());

    // 发送 HSET 命令
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        info!("Got response {:?}", data);
    }

    // 生成一个HGETALL命令
    let cmd = CommandRequest::new_hget_all("table1");
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        info!("Got response {:?}", data)
    }
    Ok(())
}