use std::{pin::Pin, sync::Arc};
use tokio_stream::wrappers::ReceiverStream;
use futures::{stream, Stream};
use crate::{CommandResponse, Publish, Subscribe, Unsubscribe, Value};
use super::topic::Topic;



/// 这个的意思是Pin住一个实现了Stream的trait object
pub type StreamingResponse = Pin<Box<dyn Stream<Item = Arc<CommandResponse>> + Send >>;

pub trait TopicService {
    fn execute(self, topic:impl Topic) -> StreamingResponse;
}


impl TopicService for Subscribe {
    fn execute(self, topic:impl Topic) -> StreamingResponse {
        let rx = topic.subscribe(self.topic);
        Box::pin(ReceiverStream::new(rx))
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, topic:impl Topic) -> StreamingResponse {
        let res = topic.unsubscribe(self.topic, self.id);
        let resp: CommandResponse = match res {
            Ok(_id) => {
                let v: Value = "取消订阅成功".into();
                v.into()
            },
            Err(e) => e.into(),
        };
        
        Box::pin(stream::once(async{Arc::new(resp.into())}))
    }
    
}

impl TopicService for Publish {
    fn execute(self, topic:impl Topic) -> StreamingResponse {
        topic.publish(self.topic, Arc::new(self.data.into()));
        let resp: Value = "发布成功".into();
        Box::pin(stream::once(async { Arc::new(resp.into())}))
    }
}

#[cfg(test)]
mod tests{
    use std::{sync::Arc, time::Duration};
    use futures::StreamExt;
    use tokio::time;
    use crate::{dispatch_stream, service::topic::{Broadcaster, Topic}, tests::{assert_res_error, assert_res_ok}, CommandRequest};
    
    
    #[tokio::test]
    async fn dispatch_publish_should_work(){
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::new_publish("topic", "hello".into());
        let res = dispatch_stream(cmd, topic).next().await.unwrap();
        assert_eq!(res.status, 200);
        assert_res_ok(&res, &["发布成功".into()], &[])
    }

    #[tokio::test]
    async fn dispatch_subscribe_should_work(){
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::new_subscribe("topic");
        let res = dispatch_stream(cmd, topic).next().await.unwrap();
        let id: i64 = res.values[0].clone().try_into().unwrap();
        assert_eq!(res.status, 200);
        println!("{:?}", res.values);
        assert_res_ok(&res, &[(id as i32).into()], &[])
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_should_work(){
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::new_subscribe("topic");
        let resp = dispatch_stream(cmd, topic.clone()).next().await.unwrap();
        println!("resp: {resp:?}");
        let id:i64 = resp.as_ref().values[0].clone().try_into().unwrap();
        let cmd = CommandRequest::new_unsubscribe("topic", id as u32);
        let res = dispatch_stream(cmd, topic).next().await.unwrap();
        assert_eq!(res.status, 200);
        println!("dispatch_unsubscribe_should_work: {:?}", res);
        assert_res_ok(&res, &["取消订阅成功".into()], &[])
    }

    #[tokio::test]
    async fn dispatch_sunscribe_abnormal_quit_should_be_removed_on_next_publish(){
        let topic = Arc::new(Broadcaster::default());
        let id = {
            let cmd = CommandRequest::new_subscribe("topic");
            let res = dispatch_stream(cmd, topic.clone()).next().await.unwrap();
            let id: i64 = res.values[0].clone().try_into().unwrap();
            drop(res);
            id as u32
        };

        // publish时这个subscription已经实效，所以会被删除
        let cmd = CommandRequest::new_publish("topic", "hello".into());
        dispatch_stream(cmd, topic.clone()).next().await.unwrap();
        time::sleep(Duration::from_millis(200)).await;

        // 如果再尝试删除,应该返回KvError
        let result = topic.unsubscribe("topic".into(), id);
        assert!(result.is_err());

    }

    #[tokio::test]
    async fn dispatch_unsubscribe_random_id_should_error(){
        let topic = Arc::new(Broadcaster::default());
        let cmd = CommandRequest::new_unsubscribe("topic", 100);
        let res = dispatch_stream(cmd, topic).next().await.unwrap();
        assert_res_error(&res, 500, "Internal error:subscription 100")
    }
}