// #![allow(unused, dead_code)]
use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use tokio::io::{AsyncRead, AsyncReadExt};
use prost::Message;
use tracing::info;

use crate::{CommandRequest, CommandResponse, KvError};

/// 四个字节
pub const LEN_LEN: usize = 4;
/// 四字节除去一个符号位,31位,frame是2g
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;

/// 如果payload超过了1436字节就做压缩
const COMPRESS_LIMIT: usize = 1436;
/// 代表压缩的bit。整个长度4字节的最高位
const COMPRESS_BIT: usize = 1 << 31;

/// 处理frame的encode/decode
pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    /// 把一个message encode成一个frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();
        if size > MAX_FRAME{
            return Err(KvError::FrameError);
        }

        // 写入长度
        buf.put_u32(size as _);

        if size > COMPRESS_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;

            // BytesMut支持逻辑上的split
            // 所以我们先把长度这四个字节拿走
            let payload = buf.split_off(LEN_LEN);
            buf.clear();

            // 处理gzip压缩，
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            // 压缩完成后，从gzip encoder吧BytesMut再拿回来
            let payload = encoder.finish()?.into_inner();
            info!("Encode a frame: Size {}({})", size, payload.len());
            // 写入压缩后的长度(第一位表示是否压缩)
            buf.put_u32((payload.len() | COMPRESS_BIT) as _);

            // 把BytesMut再合并回来
            buf.unsplit(payload);
            Ok(())
        }else {
            self.encode(buf)?;
            Ok(())
        }

    }

    /// 把一个完整的frame decode成一个message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        // 先取4字节
        let header = buf.get_u32() as usize;
        let (len, compressed) = decoder_header(header);
        //  解压缩
        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len*2);
            decoder.read_to_end(&mut buf1)?;
            // 前面get_u32已经走了4B了
            buf.advance(len);

            // decode生成相应的消息
            Ok(Self::decode(&buf1[..buf1.len()])?)

        }else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }

    }

}

fn decoder_header(header: usize) ->(usize, bool) {
    // 去掉第一位的与操作
    let len = header & !COMPRESS_BIT;
    // 根据第一位判断是否压缩
    let compressed = (header & COMPRESS_BIT) == COMPRESS_BIT;
    (len, compressed)
}

impl FrameCoder for CommandResponse {}
impl FrameCoder for CommandRequest {}

pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send
{
    let header = stream.read_u32().await? as usize;
    let (len, _) = decoder_header(header);
    // 如果没有这么大的内存就分配至少一个frame的内存
    buf.reserve(LEN_LEN + len);
    buf.put_u32(header as _);
    // 上面如果分配了内存，那么是没有初始化的，所以这里是unsafe
    unsafe{ buf.advance_mut(len) }
    // 这里读取了后，就初始化了
    stream.read_exact(&mut buf[LEN_LEN..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::Value;
    use bytes::Bytes;

    #[test]
    fn command_request_encode_decode_should_work(){
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hset("t1", "key", "value".into());
        cmd.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);
        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1)
        
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let s: Value = b"asdf".as_slice().try_into().unwrap();
        let values: Vec<Value> = vec![1.into(), "hello".into(), s];
        let res: CommandResponse = values.into();
        res.encode(&mut buf).unwrap();

        // 最高位没设置
        assert_eq!(is_compressed(&buf), false);
        let res1 = CommandResponse::decode(buf).unwrap();
        assert_eq!(res, res1)

    }

    #[test]
    fn caommand_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESS_LIMIT+1]).into();
        let res: CommandResponse = value.into();
        res.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), true);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1)

    }

    fn is_compressed(data: &[u8]) -> bool{
        if let &[v] = &data[..1] {
            v >> 7 == 1
        }else {
            false
        }
    }

    struct DummyStream{
        buf: BytesMut
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_frame_should_work(){
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("table", "key");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream{buf};

        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd1)
    }
}