
#![allow(unused)]

use std::time::Duration;
use futures::StreamExt;
use tokio_util::compat::Compat;
use anyhow::Result;
use kv::{CommandRequest, KvError, ProstClientStream, TlsClientConnector, YamuxCtrl};
use tokio::{net::TcpStream, time};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let ca_cert = include_str!("../fixtures/ca.cert");
    let client_cert = include_str!("../fixtures/client.cert");
    let client_key = include_str!("../fixtures/client.key");

    let connector = TlsClientConnector::new("kvserver.acme.inc", Some((client_cert, client_key)), Some(ca_cert))?;

    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    // 打开一个yamux stream
    let mut ctrl = YamuxCtrl::new_client(stream, None);
    let channel = "lobby";
    start_publishing(ctrl.open_stream().await?, channel)?;

    time::sleep(Duration::from_secs(2)).await;

    let stream = ctrl.open_stream().await?;
    let mut client = ProstClientStream::new(stream);

    // 生成并执行一个hset command
    let cmd = CommandRequest::new_hset("table", "key", "value1".into());
    let res = client.execute_unary(&cmd).await?;
    info!("Got response: {:?}", res);

    // 生成一个Subscribe command
    let cmd = CommandRequest::new_subscribe(channel);
    let mut stream = client.execute_streaming(&cmd).await?;
    let id = stream.id;
    info!("Finished subscribe, id: {}", id);
    start_unsubscribe(ctrl.open_stream().await?, channel, id)?;


    while let Some(Ok(data)) = stream.next().await {
        info!("Got published data: {:?}", data);
        
    }
    info!("Finished");
    Ok(())
}

fn start_publishing(stream: Compat<yamux::Stream>, name: &str) -> Result<(),KvError>{
    let cmd = CommandRequest::new_publish(name, "Hello".into());
    tokio::spawn(async move{
        time::sleep(Duration::from_secs(1)).await;
        let mut client = ProstClientStream::new(stream);
        client.execute_unary(&cmd).await.unwrap();
        println!("Finished publishing");
        info!("Finished publishing");

    });
    Ok(())
}

fn start_unsubscribe(stream: Compat<yamux::Stream>, name: &str, id: u32) -> Result<(),KvError>{
    let cmd = CommandRequest::new_unsubscribe(name, id);
    tokio::spawn(async move{
        time::sleep(Duration::from_secs(2)).await;
        let mut client = ProstClientStream::new(stream);
        client.execute_unary(&cmd).await.unwrap();
        println!("Finished unsubscribing");
    });
    Ok(())
    
}