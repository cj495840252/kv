use std::path::Path;

use sled::Db;

use crate::Storage;

#[derive(Debug)]
pub struct SledDb(Db);


impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    pub fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }
    pub fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}
/// 把 Option> flip 成 Result, E>/// 从这个函数里，你可以看到函数式编程的优雅
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
     x.map_or(Ok(None), |v| v.map(Some))
    }
impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, key);
        let result = self.0.get(name.as_bytes())?.map(|v|v.as_ref().try_into());
        flip(result)
    }

    fn set(&self, table: &str, key: String, value: crate::Value) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, &key);
        let data: Vec<u8>= value.try_into()?;
        // let data =
        let result = self.0.insert(name, data)?.map(|v|v.as_ref().try_into());
        flip(result)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, crate::KvError> {
        let name = SledDb::get_full_key(table, &key);
        Ok(self.0.contains_key(name.as_bytes())?)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<crate::Value>, crate::KvError> {
        let name = SledDb::get_full_key(table, &key);
        let res = self.0.remove(name.as_bytes())?.map(|v|v.as_ref().try_into());
        flip(res)

    }

    fn get_all(&self, table: &str) -> Result<Vec<crate::Kvpair>, crate::KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let res = self.0.scan_prefix(prefix).map(
            |v| v.into()
        ).collect();
        Ok(res)

    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let res = self.0.scan_prefix(prefix).map(
            |v| v.into()
        );
        Ok(Box::new(res))

    }
}


