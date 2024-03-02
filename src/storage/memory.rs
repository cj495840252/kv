
use dashmap::{mapref::one::Ref, DashMap};

use crate::{Kvpair, Storage, StorageIter, Value};

#[derive(Clone, Debug, Default)]
pub struct  MemTable{
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }
    fn get_or_create_table(&self, name: &str) -> Ref<String,DashMap<String, Value>> {
        match self.tables.get(name) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()
            },
        }
    }

}


impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v|v.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))    
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.remove(key).map(|(_k,v)|v.clone()))
    }

    fn get_all(&self, table: &str) -> Result<Vec<crate::Kvpair>, crate::KvError> {
        let table = self.get_or_create_table(table);
        Ok(table
            .iter()
            .map(|v| Kvpair::new(v.key(), v.value().clone()))
            .collect())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = crate::Kvpair>>, crate::KvError> {
        let table = self.get_or_create_table(table).clone();
        let iter = StorageIter::new(table.into_iter());
        Ok(Box::new(iter))
    }
}
