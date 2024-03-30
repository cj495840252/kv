use std::sync::{atomic::{AtomicU32, Ordering}, Arc};
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use crate::{CommandResponse, KvError, Value};

/// 下一个订阅ID
static NEXT_ID: AtomicU32 = AtomicU32::new(0);
const BORADCAST_CAPACITY: usize = 128;

/// 获取下一个订阅ID
fn get_next_subscribe_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync + 'static {
    /// 订阅某个主题
    fn subscribe(&self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;

    // 取消订阅某个主题
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>;

    /// 发布数据到某个主题
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

/// 用于发布和订阅主题的数据结构
#[derive(Default)]
pub struct Broadcaster {
    /// 所有的主题列表
    topics: DashMap<String, DashSet<u32>>,
    /// 订阅者
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl Topic for Arc<Broadcaster> {
    fn subscribe(&self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name.clone()).or_default();
            let id = get_next_subscribe_id();
            entry.value().insert(id);
            id
        };

        // 生成一个mpsc channel
        let (tx, rx) = mpsc::channel(BORADCAST_CAPACITY);

        let v: Value = (id as i32).into();
        // 立即发送subscription id到rx
        let tx1 = tx.clone();

        tokio::spawn(async move{
            if let Err(e) = tx1.send(Arc::new(v.into())).await {
                // TODO: 这个很小概率发生，但目前没有善后
                warn!("Failed to send subscription id: {:?}", e);
            }
        });

        // 把tx存入subscriptions table
        self.subscriptions.insert(id, tx);
        debug!("Subscribe to topic: {}", id);
        rx
    }

    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>{
        match self.remove_subscription(name, id) {
            Some(id) => Ok(id),
            None => Err(KvError::Internal(format!("subscription {}", id))),
        }
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            let mut ids = vec![];
            match self.topics.get(&name) {
                Some(topic) => {
                    // 复制整个 topic 下所有的 subscription id 
                    // 这里我们每个 id 是 u32，如果一个 topic 下有 10k 订阅，复制的成本 
                    // 也就是 40k 堆内存（外加一些控制结构），所以效率不算差 
                    // 这也是为什么我们用 NEXT_ID 来控制 subscription id 的生成
                    let subscriptions = topic.value().clone();
                    // 尽快释放锁
                    drop(topic);
                    for id in subscriptions.into_iter() {
                        if let Some(tx) = self.subscriptions.get(&id){
                            if let Err(e) = tx.send(value.clone()).await{
                                warn!("id: {:?} Failed to send data to subscriber: {:?}", id, e);
                                ids.push(id);
                            }
                        }
                    }
                },
                None => {},
            }
            for id in ids {
                self.remove_subscription(name.clone(), id);
            }
        });
    }
}

impl Broadcaster {
    pub fn remove_subscription(&self, name: String, id: u32) -> Option<u32>{
        // 在topics表里找到topic的subscriptions id 删除
        if let Some(v) = self.topics.get_mut(&name){
            v.remove(&id);
            if v.is_empty(){
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        };
        debug!("Unsubscribe from topic: {}", id);
        self.subscriptions.remove(&id).map(|(id,_)| id)
    }
}
#[cfg(test)]
mod tests{
    use tokio::time;

    use crate::tests::assert_res_ok;

    use super::*;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn pub_sub_should_work(){
        time::sleep(Duration::from_secs(1)).await;

        let b = Arc::new(Broadcaster::default());
        let lobby = "lobby".to_string();

        // subscribe
        let mut stream1 = b.clone().subscribe(lobby.clone());
        let mut stream2 = b.clone().subscribe(lobby.clone());


        // publish
        let v: Value = "hello".into();
        b.clone().publish(lobby.clone(), Arc::new(v.clone().into()));

        // subscribers 应该能收到publish的数据
        let resp1 = stream1.recv().await.unwrap();
        let id1: i64 = resp1.values[0].clone().try_into().unwrap();
        let resp2 = stream2.recv().await.unwrap();
        assert_ne!(resp1, resp2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();
        assert_eq!(res1, res2);
        assert_res_ok(&res1, &[v.clone()], &[]);

        // unsubscribe
        b.clone().unsubscribe("lobby".into(), id1 as u32).unwrap();
        let v: Value = "world".into();
        b.clone().publish("lobby".into(), Arc::new(v.clone().into()));
        let res3 = stream1.recv().await;
        let res4 = stream2.recv().await.unwrap();
        assert!(res3.is_none());
        assert_res_ok(&res4, &[v.clone()], &[]);
        
    }
}