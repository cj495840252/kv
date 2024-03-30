use std::{ops::{Deref, DerefMut}, pin::Pin};

use futures::{Stream, StreamExt};

use crate::{CommandResponse, KvError};



pub struct StreamResult{
    pub id: u32,
    /// 这一句的意思是Pin住实现Stream的trait对象
    inner: Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send >>
}

impl StreamResult {
    pub async fn new<T>(mut stream: T) -> Result<StreamResult, KvError> 
    where
        T: Stream<Item = Result<CommandResponse, KvError>> + Send + 'static + Unpin
    {
        let id = match stream.next().await {
            Some(Ok(CommandResponse{
                status: 200,
                values: v,
                ..
            })) => {
                if v.is_empty(){
                    return Err(KvError::Internal("Invalid Stream".into()));
                }
                let id: i64 = (v[0].clone()).try_into().unwrap();
                Ok(id as u32)
            },
            _ => Err(KvError::Internal("Invalid Stream".into())),
        };
        Ok(StreamResult{
            id: id?,
            inner: Box::pin(stream),
        })
    }
    
}

impl Deref for StreamResult {
    // 这一句的意思是Pin住实现Stream的trait对象
    type Target = Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send >>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
    
}

impl DerefMut for StreamResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
    
}