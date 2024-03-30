mod command_service;
mod topic_service;
mod topic;

use topic_service::TopicService;
use self::{topic::Topic, topic_service::StreamingResponse};
use std::sync::Arc;
use tracing::debug;
use crate::{CommandRequest, CommandResponse, KvError, MemTable, Notify, NotifyMut, Storage};


/// 对Command的处理的抽象
/// CommandRequest的所有枚举实现这个trait
pub trait CommandService{
    
    /// 处理Command，返回Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}


/// 具体的service， 接受一个CommandRequst,返回一个 CommandResponse
/// 内部是一个Arc的storage
pub struct Service<Store = MemTable>{
    inner: Arc<ServiceInner<Store>>,
    boradcaster: Arc<topic::Broadcaster>,
}

impl <Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) , boradcaster: Arc::clone(&self.boradcaster)}
    }
}

impl <Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self { inner: Arc::new(inner) , boradcaster: Default::default()}
    }
}

pub struct ServiceInner<Store>{
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>, 
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl <Store: Storage> ServiceInner<Store> {

    pub fn new(store: Store) -> Self {
        ServiceInner { 
            store, 
            on_received: Vec::new(), 
            on_executed: Vec::new(), 
            on_before_send: Vec::new(), 
            on_after_send: Vec::new() 
        }
    }
    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self{
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }
}

impl <Store: Storage> Service <Store> {
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Get request: {:?}", cmd);

        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd.clone(), &self.inner.store);
        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.boradcaster))
        }else {
            debug!("Executed response: {:?}", res);
            self.inner.on_executed.notify(&res);
            self.inner.on_before_send.notify(&mut res);
            if !self.inner.on_before_send.is_empty() {
                debug!("Modified response: {:?}", res)
            }
            Box::pin(futures::stream::once(async { Arc::new(res) }))
        }
    }
}

pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(req) => {
            match req {
                crate::command_request::RequestData::Hget(param) => param.execute(store),
                crate::command_request::RequestData::Hgetall(param) => param.execute(store),
                crate::command_request::RequestData::Hset(param) => param.execute(store),
                // 处理不了的返回一个啥都不包括的Response， 方便dispatch_stream处理
                _ => Default::default()
            }
        },
        None => KvError::InvalidCommand("Not a invalid command, reques is none".into()).into(),
    }
}

pub fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match cmd.request_data {
        Some(req) => {
            match req {
                crate::command_request::RequestData::Publish(param) => param.execute(topic),
                crate::command_request::RequestData::Subscribe(param) => param.execute(topic),
                crate::command_request::RequestData::Unsubscribe(param) => param.execute(topic),
                _ => unreachable!("Not implemented"),
            }
        },
        None => unreachable!("Not implemented"),
    } 
    
    
}

#[cfg(test)]
pub(crate) mod tests{
    use std::thread::spawn;

    use super::*;
    use futures::StreamExt;
    use tracing::info;
    use crate::{CommandRequest, MemTable};

    #[tokio::test]
    async fn service_should_work(){
        // 我们需要一个结构体，至少包含Storege;
        let service: Service = ServiceInner::new(MemTable::default()).into();
        // service可以运行在多线程下，它的cloned应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程， 在table t1中写入k1, v1
        let handle = spawn(|| async move{
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[])
        });
        handle.join().unwrap().await;

        // 在当前线程下读取
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {:?}", cmd);
        }
        fn c(res: &CommandResponse) {
            info!("{:?}", res);
        }
        fn d(res: &mut CommandResponse) {
            res.status = 200;
        }
        fn e() {
            info!("Data is sent");
        }
    
        let service: Service = ServiceInner::new(MemTable::default())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();
    
        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let data = res.next().await.unwrap();
        assert_eq!(data.status, 200);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }

    use crate::{ Kvpair, Value };

    pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        let mut res_pairs = res.pairs.clone();
        res_pairs.sort_by(|a,b|a.partial_cmp(b).unwrap());

        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res_pairs, pairs);
    }
    
    pub fn assert_res_error(res: &CommandResponse, status: u32, msg: &str) {
        assert_eq!(res.status, status);
        assert!(res.message.contains(msg));
        assert_eq!(res.pairs, &[]);
        assert_eq!(res.values,&[])
    }
    
}


