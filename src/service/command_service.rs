use crate::*;



impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}


impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(Kvpair { key, value }) => {
                match store.set(&self.table, key, value.unwrap()) {
                    Ok(Some(v)) => v.into(),
                    Ok(None) => Value::default().into(),
                    Err(e) => e.into(),
                }
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl From<Value> for CommandResponse {
    fn from(value: Value) -> Self {
        Self {
            status: 200,
            values: vec![value],
            ..Default::default()
        }   
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(value: Vec<Kvpair>) -> Self {
        Self {
            status: 200,
            pairs: value,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self{
            status: 1,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],

        };

        match e {
            KvError::NotFound(_, _) => result.status = 404,
            KvError::InvalidCommand(_) => result.status = 500,
            _ => {}
        }
        result
    }
}


#[cfg(test)]
mod tests{
    use super::{MemTable, CommandRequest, CommandService, CommandResponse, Kvpair, Storage};
    use crate::command_request::RequestData;
    use crate::service::tests::{assert_res_error, assert_res_ok};
    use crate::Value;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[])
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("t1","hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);

        let cmd = CommandRequest::new_hget("score","u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not Found");

    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("t1", "key");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404,"Not Found")

    }


    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hget_all("score");
        let res = dispatch(cmd, &store);

        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs)

    }


    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse{
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            _ => todo!(),
        }
    }
}