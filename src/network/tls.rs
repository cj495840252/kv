use std::{io::Cursor, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{
        client::TlsStream as ClientTlsStream, 
        server::TlsStream as ServerTlsStream, 
        rustls::{
            internal::pemfile::{self, pkcs8_private_keys, rsa_private_keys}, AllowAnyAuthenticatedClient, Certificate, 
            ClientConfig, NoClientAuth, PrivateKey, RootCertStore, ServerConfig
        }, 
        webpki::DNSNameRef, TlsAcceptor, TlsConnector
    };

use crate::KvError;

/// kv Server自己的ALPN(Application-layer Protocol Negotiation)
const ALPN_KV: &str = "kv";


/// 存放TLS ServerConfig并提供accept把底层的协议转成TLS
#[derive(Clone)]
pub struct TlsServerAcceptor{
    inner: Arc<ServerConfig>,
}

/// 存放TLS Client并提供connect方法把底层的协议转成TLS
#[derive(Clone)]
pub struct TlsClientConnector{
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>
}

impl TlsClientConnector {
    /// 加载client cert/ CA cert，生产ClientConfig
    pub fn new(domain: impl Into<String>, identity: Option<(&str, &str)>, server_ca: Option<&str>) -> Result<Self, KvError>{
        let mut config = ClientConfig::new();
        
        // 如果有客户端证书，加载之
        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            config.set_single_client_cert(certs, key)?;
        };

        // 加载本地信任的根证书链
        config.root_store = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store),_)) => store,
            Err((None, error)) => return Err(error.into()),
        };

        // 如果有签署服务器的CA证书，则加载它，这样服务器不存在根证书链
        // 但是这个CA证书能验证它，也可以
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        }

        Ok(Self { config: Arc::new(config), domain: Arc::new(domain.into()) })
        
    }

    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError> 
    where 
        S: AsyncRead + AsyncWrite + Unpin + Send
    {
        let dns = DNSNameRef::try_from_ascii_str(&self.domain.as_str())
        .map_err(|_|KvError::Internal("Invalid DNS Name".into()))?;

        let stream = TlsConnector::from(self.config.clone()).connect(dns, stream).await?;
        Ok(stream)
    }
}

impl TlsServerAcceptor {
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;

        let mut config = match client_ca {
            Some(cert) => {
                let mut cert = Cursor::new(cert);
                let mut client_root_cert_store = RootCertStore::empty();
                client_root_cert_store.add_pem_file(&mut cert)
                .map_err(|_|KvError::CertifacteParserError("CA", "cert"))?;
                let client_path = AllowAnyAuthenticatedClient::new(client_root_cert_store);
                ServerConfig::new(client_path)
            },
            None => ServerConfig::new(NoClientAuth::new()),
        };

        // 加载服务器证书
        config.set_single_cert(certs, key).map_err(|_|KvError::CertifacteParserError("sever", "cert"))?;
        config.set_protocols(&[Vec::from(&ALPN_KV[..])]);
        Ok(Self { inner: Arc::new(config) })
    }

    ///出发TLS协议,将底层的stream转换成TLS Stream
    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError> 
    where
        S: AsyncRead + AsyncWrite + Unpin + Send
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor.accept(stream).await?)
    }
}


fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert);
    pemfile::certs(&mut cert).map_err(|_|KvError::CertifacteParserError("server","cert"))
    
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut cursor = Cursor::new(key);
    // 先尝试用PKCS8加载私钥
    if let Ok(mut keys) = pkcs8_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    // 再尝试加载RSA key
    cursor.set_position(0);
    if let Ok(mut keys) = rsa_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    // 不支持的私钥类型
    Err(KvError::CertifacteParserError("private", "key"))
}


#[cfg(test)]
mod tests{

    #![allow(unused)]
    
    use tokio::net::TcpStream;
    use std::net::SocketAddr;
    use anyhow::Result;
    use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

    use crate::network::tls::TlsClientConnector;

    use super::TlsServerAcceptor;
    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    #[tokio::test]
    async fn tls_should_work() -> Result<()>{
        let ca = Some(CA_CERT);
        let addr = start_server(None).await?;

        let connector = TlsClientConnector::new("kvserver.acme.inc", None, ca)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0;12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())

    }


    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        println!("start test");

        let client_identity = Some((CLIENT_CERT,CLIENT_KEY));
        let ca = Some(CA_CERT);
        println!("开启服务");
        let addr = start_server(ca.clone()).await?;
        println!("Listening addr {:?}", addr);
        let connector = TlsClientConnector::new("kvserver.acme.inc", client_identity, ca)?;
        let stream = TcpStream::connect(addr).await.unwrap();
        // stream.write_all(b"hello")
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;

        let mut buf = [0;12];
        stream.read_exact(&mut buf).await?;

        assert_eq!(&buf, b"hello world!");



        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_not_work() -> Result<()>{
        let addr = start_server(None).await?;
        let connector = TlsClientConnector::new("kvserver1.acme.inc", None, Some(CA_CERT))?;

        let stream = TcpStream::connect(addr).await?;
        let result = connector.connect(stream).await;
        assert!(result.is_err());
        Ok(())
    }

    async fn start_server(ca: Option<&str>) -> Result<SocketAddr>{
        let acceptor = TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca)?;
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move{
            let (stream,_) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0;12];
            println!("receive message ...");
            stream.read_exact(&mut buf).await.unwrap();
            println!("{}", std::str::from_utf8(&buf[..]).unwrap());
            stream.write_all(&buf).await.unwrap();
        });
        Ok(addr)
        
    }
}