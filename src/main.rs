// Copyright (c) 2023 Anssi Eteläniemi
extern crate sqlite;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::str::from_utf8;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{
    Engine as _,
    engine::general_purpose,
};
use clap::arg;
use futures::StreamExt;
use hyper::{body, Body, Client, Response, StatusCode};
use hyper::body::Bytes;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use sqlite::{Connection, CursorWithOwnership, Row};
//use tokio::stream;
use tokio::time::{Instant, sleep};
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{debug, error, Level};
use tracing::{info, trace};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber;
use tracing_subscriber::{EnvFilter, fmt};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
#[allow(unused_imports)] // Import needed
use tracing_subscriber::util::SubscriberInitExt;

const TRACING_APPENDER_DIRECTORY: &'static str = "./";
const TRACING_APPENDER_PREFIX: &'static str = "encoding-fetcher.log";

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
#[derive(Debug)]
struct Cli {
    // Build URL
    #[arg(default_value_t = String::from("http://127.0.0.1:3000/files/APIName.php/150491277/1/DeaDJmBeefFhb00B5/getInfo"))]
    path: String,
    #[arg(long)]
    product_number: String,

    // DB
    #[arg(long, default_value_t = String::from("uid.db"))]
    db_path: String,

    // Options
    #[arg(long,default_value_t = false)]
    swap_uid_endianness: bool,
    #[arg(long,default_value_t = false)]
    remove_gaps: bool,
    #[arg(long,default_value_t = false)]
    reverse_input: bool,
    #[arg(long,default_value_t = true)]
    send_delay: bool,
    #[arg(long, default_value_t = -1)]
    ticket_amount: i64,
    #[arg(long, default_value_t = 1)]
    serial_mapping: i64,
    #[arg(long, default_value_t = 0)]
    skip_first_count: i64,
    #[arg(long, default_value_t = 5)]
    retry_count: usize,
    #[arg(long, default_value_t = 3)]
    parallel: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Ticket {
    #[serde(rename = "cardUID")]
    card_uid: String,
    start_byte_position: String,
    byte_length: String,
    byte_string_base64: String,
}

fn make_url(sn_i64: i64, uid: &String, path: &String, pn: &String) -> String {
    let url = format!("{path}/{uid}/{pn}/{sn_i64:0>6}");
    url
}

fn get_cursor(connection: &Connection, skip_first_count: i64, ticket_amount: i64, serial_mapping: i64, remove_gaps: bool, reverse_input:bool) -> impl Iterator<Item=sqlite::Row> + '_ {
    let res = get_cursor_with_limit(connection, serial_mapping, skip_first_count, ticket_amount, remove_gaps, reverse_input);
    res.map(|row| row.unwrap())
}

fn get_cursor_with_limit(connection: &Connection, serial_mapping: i64, skip_first_count: i64, ticket_amount: i64, remove_gaps: bool, reverse_input: bool) -> CursorWithOwnership {
    let status = "GOOD";
    let direction = if reverse_input {
        "DESC"
    } else {
        "ASC"
    };
    let query = if remove_gaps {
        format!("SELECT RANK () OVER ( ORDER BY id {direction} ) + ? GAPLESS_ID, UIDTID FROM TICKET WHERE STATUS = '{status}' limit ?, ?")
    } else {
        format!("SELECT ID + ?, UIDTID FROM TICKET WHERE STATUS = '{status}' ORDER BY ID {direction} limit ?, ?")
    };
    connection
        .prepare(query)
        .expect("Check db-path")
        .into_iter()
        .bind((1, serial_mapping - 1)).unwrap()
        .bind((2, skip_first_count)).unwrap()
        .bind((3,ticket_amount)).unwrap()
}

async fn make_file_row(body: Response<Body>) -> Result<(String, String)> {
    let data = body::to_bytes(body.into_body())
        .await
        .expect("Failed to read Body");
    debug!("{:?}", data);
    decode_json(&data)
}

fn decode_json(data: &Bytes) -> Result<(String, String)> {
    let t: Ticket = serde_json::from_slice(data.as_ref())?;
    let encoding = general_purpose::STANDARD
        .decode(t.byte_string_base64)
        .expect("Failed to decode base64");
    Ok((t.card_uid, from_utf8(&encoding).unwrap().parse().unwrap()))
}

fn init_tracing() -> WorkerGuard {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("debug,hyper=info"));
    let file_appender =
        tracing_appender::rolling::daily(TRACING_APPENDER_DIRECTORY, TRACING_APPENDER_PREFIX);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::Layer::new()
                .with_writer(std::io::stdout.with_max_level(Level::INFO))
                .pretty(),
        )
        .with(
            fmt::Layer::new()
                .with_writer(
                    non_blocking,
                )
                .json(),
        );
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");
    info!("Tracing initialized at {TRACING_APPENDER_DIRECTORY}{TRACING_APPENDER_PREFIX}");
    guard
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> core::result::Result<(), ()> {
    use clap::Parser;
    let cli = Cli::parse();
    let _tracing_guard = init_tracing();
    info!("Starting with parameters {:?}", cli);
    let file = &create_data_file(&cli.product_number);
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);

    // Actually open or creates so check the path in the case of table does not found
    let connection = sqlite::open(&cli.db_path).expect("Unable to open database");
    let cursor = get_cursor(&connection, cli.skip_first_count, cli.ticket_amount, cli.serial_mapping, cli.remove_gaps, cli.reverse_input);
    let now = Instant::now();
    let bodies = futures::stream::iter(cursor)
        .map(|row| {
            let client = &client;
            let cli = &cli;
            async move {
                let mapped_serial: i64 = row.read::<i64, _>(0);
                let res = process_entry(&cli, &client, row, mapped_serial).await;
                match res {
                    Ok(resp) => {
                        Ok((mapped_serial, resp))
                    },
                    Err(_) => {Err(mapped_serial)}
                }
            }
        })
        .buffered(cli.parallel);
    let mut total_count = 0;
    static FAILED_COUNT: AtomicUsize = AtomicUsize::new(0);
    bodies
        .for_each(|resp| {
            total_count += 1;
            async move {
                 match resp {
                    Ok((mapped_serial, response)) => {
                        let file_row = make_file_row(response).await;
                        let _ok = write_file_row(file, mapped_serial, file_row).await;
                    },
                     Err(mapped_serial) => {
                         FAILED_COUNT.fetch_add(1, Ordering::SeqCst);
                         error!("Failed write data file entry for serial: {mapped_serial}");
                     },
                }
            }
        }).await;
    let failed_count = FAILED_COUNT.load(Ordering::SeqCst);
    info!(
        "Total entries of {} took {}ms, failed: {}",
        total_count,
        now.elapsed().as_millis(),
        failed_count
    );
    return Ok(());
}

async fn process_entry(cli: &Cli, client: &Client<HttpsConnector<HttpConnector>>, row: Row, mapped_serial: i64) -> std::result::Result<Response<Body>, StatusCode> {
    let retry_strategy = ExponentialBackoff::from_millis(10)
        .map(jitter) // add jitter to delays
        .take(cli.retry_count);
    let uid = if cli.swap_uid_endianness {
        swap_uid_endianness(row.read::<&str, _>("UIDTID"))
    } else {
        String::from(row.read::<&str, _>("UIDTID"))
    };
    let url = make_url(mapped_serial, &uid, &cli.path, &cli.product_number);
    debug!("{:?}", url);
    let mut retry_counter: Option<i32> = None;
    Retry::spawn(retry_strategy, || {
        if let Some(mut count) = retry_counter {
            info!("Retry {count} for {url}");
            count += 1;
            retry_counter.replace(count);
        } else {
            retry_counter = Some(1);
        }
        let mut first_delay = None;
        if cli.send_delay {
            let first_delay_ms = rand::random::<u64>() % 200;
            first_delay = Some(Duration::from_millis(first_delay_ms));
        }
        get_body_handle_err(client, &url, first_delay)
    })
        .await
}

fn swap_uid_endianness(uid: &str) -> String {
    assert_eq!(uid.len() % 2, 0);
    let swapped_uid = uid
        .chars()
        .collect::<Vec<char>>()
        .chunks(2)
        .map(|c2| c2.iter().collect::<String>())
        .rev()
        .collect::<Vec<String>>()
        .join("");
    trace!("Swapped UID endianness from {uid} to {swapped_uid}");
    swapped_uid
}

async fn get_body_handle_err(
    client: &Client<HttpsConnector<HttpConnector>>,
    url: &String,
    first_delay: Option<Duration>,
) -> std::result::Result<Response<Body>, StatusCode> {
    if let Some(delay) = first_delay {
        sleep(delay).await;
    }
    let result = get_body(&client, url).await;
    let status = result.status();
    if status != StatusCode::OK {
        error!("Server returned: {status}: {url}");
        return Err(status);
    }
    return Ok(result);
}

static FILE_MUTEX: Mutex<i32> = Mutex::new(0);

#[tracing::instrument]
async fn write_file_row(
    mut file: &File,
    mapped_serial: i64,
    file_row: Result<(String, String)>,
) -> std::result::Result<(), ()> {
    let (uid, enc) = file_row.unwrap();
    let guard = FILE_MUTEX.lock();
    if let Err(_) = writeln!(&mut file, "{},{:0>6},{}", uid, mapped_serial, enc) {
        error!("Couldn't write to file {mapped_serial}");
        return Err(());
    }
    file.flush().expect("Failed to flush file");
    drop(guard);
    Ok(())
}

#[tracing::instrument]
fn create_data_file(product_number: &String) -> File {
    let secs_since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    let file_name = &format!("data_{}_{:?}.txt", product_number, secs_since_the_epoch);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .expect("Creating file failed");
    info!("Created output file: {file_name}");
    file
}

async fn get_body(client: &Client<HttpsConnector<HttpConnector>>, url: &String) -> Response<Body> {
    let result = client
        .get(url.parse().expect("Failed to parse url req"))
        .await;
    match result {
        Ok(resp) => { resp }
        Err(err) => {
            error!("Failed to fetch URL: {err}");
            Response::builder().status(StatusCode::BAD_REQUEST).body(Body::empty()).unwrap()
        }
    }
}
