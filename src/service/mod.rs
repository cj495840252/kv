use std::sync::Arc;
use tracing::debug;
use crate::{CommandRequest, CommandResponse, KvError, MemTable, Notify, NotifyMut, Storage};

mod command_service;

/// 对Command的处理的抽象
/// CommandRequest的所有枚举实现这个trait
/// 
pub trait CommandService{
    
    /// 处理Command，返回Response
    fn execute(self, store: &impl Storage) -> CommandResponse;
}


/// 具体的service， 接受一个CommandRequst,返回一个 CommandResponse
/// 内部是一个Arc的storage
pub struct Service<Store = MemTable>{
    inner: Arc<ServiceInner<Store>>,
}

impl <Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl <Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self { inner: Arc::new(inner) }
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
    // pub fn new(store: Store) -> Self {
    //     Self {
    //         inner: Arc::new(ServiceInner::new(store))
    //     }
    // }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Get request: {:?}", cmd);
        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd, &self.inner.store);
        debug!("Executed response: {:?}", res);
        self.inner.on_executed.notify(&res);
        self.inner.on_before_send.notify(&mut res);
        if !self.inner.on_before_send.is_empty() {
            debug!("Modified response: {:?}", res)
        }
        res
    }
}

pub fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(req) => {
            match req {
                crate::command_request::RequestData::Hget(param) => param.execute(store),
                crate::command_request::RequestData::Hgetall(param) => param.execute(store),
                crate::command_request::RequestData::Hset(param) => param.execute(store),
                _ => KvError::Internal("Not implemented".into()).into()
            }
        },
        None => KvError::InvalidCommand("Not a invalid command, reques is none".into()).into(),
    }
}

#[cfg(test)]
pub(crate) mod tests{
    use std::thread::spawn;

    use super::*;
    use tracing::info;
    use crate::{CommandRequest, MemTable};

    #[test]
    fn service_should_work(){
        // 我们需要一个结构体，至少包含Storege;
        let service: Service = ServiceInner::new(MemTable::default()).into();
        // service可以运行在多线程下，它的cloned应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程， 在table t1中写入k1, v1
        let handle = spawn(move ||{
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[])
        });
        handle.join().unwrap();

        // 在当前线程下读取
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }

    #[test]
    fn event_registration_should_work() {
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
    
        let res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }

    use crate::{ Kvpair, Value };

    pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        res.pairs.sort_by(|a,b|a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res.pairs, pairs);
    }
    
    pub fn assert_res_error(res: CommandResponse, status: u32, msg: &str) {
        assert_eq!(res.status, status);
        assert!(res.message.contains(msg));
        assert_eq!(res.pairs, &[]);
        assert_eq!(res.values,&[])
    }
    
}


