pub mod abi;
use abi::{command_request::RequestData, *};
use bytes::Bytes;
use prost::Message;
use sled::{Error, IVec};

use crate::KvError;

impl CommandRequest {
    /// 创建 HSET 命令
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }
    /// 创建HGET
    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self{
            request_data: Some(RequestData::Hget(Hget { table: table.into(), key: key.into() }))
        }
    }

    /// 创建HGETALL
    pub fn new_hget_all(table: impl Into<String>)->Self{
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall { table: table.into() }))
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(value: Vec<Value>) -> Self {
        Self{
            status: 200,
            message: "default from Value".into(),
            values: value,
            pairs: Default::default(),
        }
    }
}
impl Kvpair {
    /// 创建一个新的 kv pair
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

/// 从 String 转换成 Value
impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

/// 从 &str 转换成 Value
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into())),
        }
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self {
            value: Some(value::Value::Integer(value as i64))
        }
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(value: (String, Value)) -> Self {
        Kvpair::new(value.0, value.1)
    }
}
impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self { value: Some(value::Value::Binary(value)) }
    }
}

impl <const N: usize> From<&[u8;N]> for Value {
    fn from(buf: &[u8;N]) -> Self {
        Bytes::copy_from_slice(&buf[..]).into()
    }
}

impl TryFrom<&[u8]> for Value{
    type Error = KvError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let res = std::str::from_utf8(value)
        .map_err(|e| KvError::Internal(e.to_string()))?;
        Ok(res.into())
    }
}

impl TryFrom<Vec<u8>> for Value {
    type Error = KvError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let res = std::str::from_utf8(&value)
        .map_err(|e| KvError::Internal(e.to_string()))?;
        Ok(res.into())
    }
}

impl TryInto<Vec<u8>> for Value {
    type Error = KvError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let str = match self.value {
            Some(val) => match val {
                value::Value::String(v) => v.to_string().as_bytes().to_vec(),
                value::Value::Binary(v) => v.to_vec(),
                value::Value::Integer(v) => v.to_string().as_bytes().to_vec(),
                value::Value::Float(v) => v.to_string().as_bytes().to_vec(),
                value::Value::Bool(v) => v.to_string().as_bytes().to_vec(),
            },
            None => "".to_string().encode_to_vec(),
        };
        Ok(str)
    }
}

impl From<Result<(IVec, IVec), Error>> for Kvpair {
    fn from(value: Result<(IVec, IVec), Error>) -> Self {
        match value {
            Ok((k,v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            Err(_) => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = std::str::from_utf8(ivec).unwrap();
    let mut iter = s.split(":");
    iter.next();
    iter.next().unwrap()
}