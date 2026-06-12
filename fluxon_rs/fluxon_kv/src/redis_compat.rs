use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use limit_thirdparty::tokio;
use tokio::io::AsyncWriteExt;

use crate::Framework;

const RESP_OK: &[u8] = b"+OK\r\n";
const RESP_PONG: &[u8] = b"+PONG\r\n";
const RESP_NULL_BULK: &[u8] = b"$-1\r\n";
const RESP_EMPTY_ARRAY: &[u8] = b"*0\r\n";

type InMemStore = Arc<RwLock<HashMap<Vec<u8>, Bytes>>>;

pub(crate) fn start_redis_compat_server(
    framework: Arc<Framework>,
    listen_addr: SocketAddr,
) -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(listen_addr)?;
    listener.set_nonblocking(true)?;

    // Benchmark-only: bypass FluxonKV and serve a pure in-memory map so we can quantify
    // the RESP parsing/IO overhead separately from the distributed KV path.
    let store: InMemStore = Arc::new(RwLock::new(HashMap::new()));

    let view = framework.cluster_manager_view();
    let _ = view.spawn("redis_compat_server", async move {
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("redis_compat: from_std failed: {}", e);
                return;
            }
        };
        let mut shutdown_waiter = framework.cluster_manager_view().register_shutdown_waiter();
        loop {
            tokio::select! {
                _ = shutdown_waiter.wait() => {
                    break;
                }
                res = listener.accept() => {
                    let (stream, peer) = match res {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!("redis_compat: accept failed: {}", e);
                            continue;
                        }
                    };
                    let fw = framework.clone();
                    let store = store.clone();
                    let v = fw.cluster_manager_view();
                    let _ = v.spawn(format!("redis_compat_conn_{}", peer), async move {
                        if let Err(e) = handle_conn(fw, store, stream).await {
                            tracing::warn!("redis_compat: conn {} failed: {}", peer, e);
                        }
                    });
                }
            }
        }
    });
    Ok(())
}

async fn handle_conn(
    framework: Arc<Framework>,
    store: InMemStore,
    stream: tokio::net::TcpStream,
) -> std::io::Result<()> {
    use tokio::io::{BufReader, BufWriter};
    let (rd, wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut writer = BufWriter::new(wr);
    let mut shutdown_waiter = framework.cluster_manager_view().register_shutdown_waiter();

    loop {
        let cmd = tokio::select! {
            _ = shutdown_waiter.wait() => {
                break;
            }
            res = read_command(&mut reader) => res,
        };
        let argv_opt = match cmd {
            Ok(v) => v,
            Err(e) => {
                write_error(&mut writer, &format!("protocol error: {}", e)).await?;
                writer.flush().await?;
                break;
            }
        };
        let Some(argv) = argv_opt else {
            break;
        };

        let is_quit = is_quit_cmd(&argv);
        dispatch_and_write(&mut writer, &store, argv).await?;
        // `read_command` uses `BufReader` internally; when the reader still has buffered
        // bytes, it usually means the peer is pipelining. Avoid flushing per command in
        // that case to reduce syscalls, but flush before waiting for the next read so
        // non-pipelined clients don't stall.
        if is_quit || reader.buffer().is_empty() {
            writer.flush().await?;
        }
        if is_quit {
            break;
        }
    }
    Ok(())
}

fn is_quit_cmd(argv: &[Option<Vec<u8>>]) -> bool {
    let Some(Some(cmd)) = argv.get(0) else {
        return false;
    };
    cmd.eq_ignore_ascii_case(b"QUIT")
}

fn format_usize_decimal(mut n: usize, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[19] = b'0';
        return &buf[19..];
    }
    let mut i: usize = 20;
    while n > 0 {
        i -= 1;
        buf[i] = (n % 10) as u8 + b'0';
        n /= 10;
    }
    &buf[i..]
}

fn format_i64_decimal(mut n: i64, buf: &mut [u8; 32]) -> &[u8] {
    if n == 0 {
        buf[31] = b'0';
        return &buf[31..];
    }
    let neg = n < 0;
    if neg {
        n = -n;
    }
    let mut i: usize = 32;
    let mut v = n as u64;
    while v > 0 {
        i -= 1;
        buf[i] = (v % 10) as u8 + b'0';
        v /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

async fn write_error<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    s: &str,
) -> std::io::Result<()> {
    writer.write_all(b"-ERR ").await?;
    writer.write_all(s.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    Ok(())
}

async fn write_integer<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    n: i64,
) -> std::io::Result<()> {
    let mut buf = [0u8; 32];
    let digits = format_i64_decimal(n, &mut buf);
    writer.write_all(b":").await?;
    writer.write_all(digits).await?;
    writer.write_all(b"\r\n").await?;
    Ok(())
}

async fn write_bulk<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    b: &[u8],
) -> std::io::Result<()> {
    let mut buf = [0u8; 20];
    let n = format_usize_decimal(b.len(), &mut buf);
    writer.write_all(b"$").await?;
    writer.write_all(n).await?;
    writer.write_all(b"\r\n").await?;
    writer.write_all(b).await?;
    writer.write_all(b"\r\n").await?;
    Ok(())
}

async fn write_array_bulk<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    elems: &[&[u8]],
) -> std::io::Result<()> {
    let mut buf = [0u8; 20];
    let n = format_usize_decimal(elems.len(), &mut buf);
    writer.write_all(b"*").await?;
    writer.write_all(n).await?;
    writer.write_all(b"\r\n").await?;
    for e in elems {
        write_bulk(writer, e).await?;
    }
    Ok(())
}

async fn dispatch_and_write<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    store: &InMemStore,
    mut argv: Vec<Option<Vec<u8>>>,
) -> std::io::Result<()> {
    if argv.is_empty() || argv[0].is_none() {
        return write_error(writer, "empty command").await;
    }
    let cmd = argv[0].as_ref().unwrap().as_slice();
    let argc = argv.len() - 1;

    if cmd.eq_ignore_ascii_case(b"PING") {
        if argc == 0 {
            return writer.write_all(RESP_PONG).await;
        }
        if argc == 1 && argv[1].is_some() {
            return write_bulk(writer, argv[1].as_ref().unwrap()).await;
        }
        return write_error(writer, "PING takes at most 1 argument").await;
    }

    if cmd.eq_ignore_ascii_case(b"QUIT") {
        return writer.write_all(RESP_OK).await;
    }

    if cmd.eq_ignore_ascii_case(b"CONFIG") {
        if argc == 2
            && argv[1].is_some()
            && argv[2].is_some()
            && argv[1].as_ref().unwrap().eq_ignore_ascii_case(b"GET")
        {
            let pat = argv[2].as_ref().unwrap().as_slice();
            if pat.eq_ignore_ascii_case(b"*") {
                return write_array_bulk(writer, &[b"save", b"", b"appendonly", b"no"]).await;
            }
            if pat.eq_ignore_ascii_case(b"save") {
                return write_array_bulk(writer, &[b"save", b""]).await;
            }
            if pat.eq_ignore_ascii_case(b"appendonly") {
                return write_array_bulk(writer, &[b"appendonly", b"no"]).await;
            }
            return writer.write_all(RESP_EMPTY_ARRAY).await;
        }
        return write_error(writer, "unsupported command: CONFIG").await;
    }

    if cmd.eq_ignore_ascii_case(b"GET") {
        if argc != 1 || argv[1].is_none() {
            return write_error(writer, "GET requires 1 key").await;
        }
        let key = argv[1].as_ref().unwrap();
        let v_opt: Option<Bytes> = { store.read().get(key.as_slice()).cloned() };
        match v_opt {
            Some(v) => write_bulk(writer, v.as_ref()).await?,
            None => writer.write_all(RESP_NULL_BULK).await?,
        }
        return Ok(());
    }

    if cmd.eq_ignore_ascii_case(b"SET") {
        if argc != 2 || argv[1].is_none() || argv[2].is_none() {
            return write_error(writer, "SET requires key and value").await;
        }
        let key = argv[1].take().expect("checked by argv[1].is_some()");
        let value = argv[2].take().expect("checked by argv[2].is_some()");
        store.write().insert(key, Bytes::from(value));
        writer.write_all(RESP_OK).await?;
        return Ok(());
    }

    if cmd.eq_ignore_ascii_case(b"EXISTS") {
        if argc == 0 || argv[1..].iter().any(|a| a.is_none()) {
            return write_error(writer, "EXISTS requires at least 1 key").await;
        }
        let n: i64 = {
            let mut n: i64 = 0;
            let g = store.read();
            for a in &argv[1..] {
                let key = a.as_ref().expect("checked by argv[1..].iter()");
                if g.contains_key(key.as_slice()) {
                    n += 1;
                }
            }
            n
        };
        return write_integer(writer, n).await;
    }

    if cmd.eq_ignore_ascii_case(b"DEL") {
        if argc == 0 || argv[1..].iter().any(|a| a.is_none()) {
            return write_error(writer, "DEL requires at least 1 key").await;
        }
        let n: i64 = {
            let mut n: i64 = 0;
            let mut g = store.write();
            for a in &argv[1..] {
                let key = a.as_ref().expect("checked by argv[1..].iter()");
                if g.remove(key.as_slice()).is_some() {
                    n += 1;
                }
            }
            n
        };
        return write_integer(writer, n).await;
    }

    let cmd_desc = String::from_utf8_lossy(cmd);
    write_error(writer, &format!("unsupported command: {}", cmd_desc)).await
}

async fn read_command<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
) -> std::io::Result<Option<Vec<Option<Vec<u8>>>>> {
    let line = read_crlf_line(reader).await?;
    if line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty request line",
        ));
    }
    if line[0] != b'*' {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected array",
        ));
    }
    let n: i64 = std::str::from_utf8(&line[1..])
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "array len not utf8"))?
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "array len invalid"))?;
    if n < 0 {
        return Ok(None);
    }
    let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        out.push(read_bulk_or_simple(reader).await?);
    }
    Ok(Some(out))
}

async fn read_bulk_or_simple<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
) -> std::io::Result<Option<Vec<u8>>> {
    use tokio::io::AsyncReadExt;
    let line = read_crlf_line(reader).await?;
    if line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty element header",
        ));
    }
    match line[0] {
        b'$' => {
            let ln: i64 = std::str::from_utf8(&line[1..])
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "bulk len not utf8")
                })?
                .parse()
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "bulk len invalid")
                })?;
            if ln == -1 {
                return Ok(None);
            }
            if ln < -1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bulk len invalid",
                ));
            }
            let mut buf = vec![0u8; ln as usize];
            reader.read_exact(&mut buf).await?;
            let crlf = read_exact_2(reader).await?;
            if crlf != [b'\r', b'\n'] {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bulk missing CRLF",
                ));
            }
            Ok(Some(buf))
        }
        b'+' => Ok(Some(line[1..].to_vec())),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported element type",
        )),
    }
}

async fn read_crlf_line<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncBufReadExt;
    let mut buf: Vec<u8> = Vec::new();
    let n = reader.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    if buf.len() < 2 || buf[buf.len() - 2] != b'\r' || buf[buf.len() - 1] != b'\n' {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing CRLF",
        ));
    }
    buf.truncate(buf.len() - 2);
    Ok(buf)
}

async fn read_exact_2<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
) -> std::io::Result<[u8; 2]> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}
