use std::error::Error;
use std::path::Path;

use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::apiv1::conn_pool::PUBSUB;
use google_cloud_gax::grpc::Status;
use google_cloud_gax::conn::Environment;

use futures_util::StreamExt;
use google_cloud_pubsub::subscription::MessageStream;
use serde::{Deserialize, Serialize};

use base64::Engine;
use base64::engine::general_purpose;

use rusqlite::{Connection, params};


const NUM_ITERS: u64 = 100;

// using a single thread
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let project_id = String::from("test-project");
    let host = String::from("0.0.0.0:8085");
    let config = ClientConfig {
        pool_size: None,
        project_id: Some(project_id),
        environment: Environment::Emulator(host),
        endpoint: String::from(PUBSUB),
    };
    run(config).await.unwrap();
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Data {
    V1 { response_bytes_utf8: String },
    V2 { response_str: String },
}

#[derive(Deserialize, Serialize, Debug)]
struct Scan {
	ip: String,
	port: u32,
	service: String,
	timestamp: i64,
    // 1 or 2, option to avoid adding another type for simplicity. couldn't figure out how to get "normal" enum serialization to work.
	data_version: Option<i64>,
	data: Data,
    // extra field for database
    version: Option<i64>
}

fn upsert_scan(conn: &Connection, scan: Scan) -> Result<(), Box<dyn Error>> {
    let content = match scan.data {
        // could decode this by writing custom deserializer function, but this is simpler for now
        Data::V1 { response_bytes_utf8 } => String::from_utf8(general_purpose::STANDARD.decode(response_bytes_utf8)?)?,
        Data::V2 { response_str } => response_str
    };
    conn.execute(
        "INSERT INTO scans (ip, port, service, timestamp, data, version)
         VALUES (?1, ?2, ?3, ?4, ?5, 1)
         ON CONFLICT(ip, port, service)
         DO UPDATE SET
            data = CASE WHEN excluded.timestamp > scans.timestamp THEN excluded.data ELSE scans.data END,
            version = CASE WHEN excluded.timestamp > scans.timestamp THEN scans.version + 1 ELSE scans.version END,
            timestamp = CASE WHEN excluded.timestamp > scans.timestamp THEN excluded.timestamp ELSE scans.timestamp END",
        params![&scan.ip, &scan.port, &scan.service, &scan.timestamp, &content],
    )?;
    Ok(())
}

// side effect: modifies database via conn
fn process_message(data: &Vec<u8>, conn: &Connection) -> Result<(), Box<dyn Error>> {
    // migrations for 2 data versions: we just do this simply and manually now
    // could look into automatic schema migration or setup our own lightweight migration handler
    let scan: Scan = serde_json::from_slice(data)?;
    upsert_scan(conn, scan)?;
    Ok(())
}

fn setup_db(path: &Path) -> Result<Connection, Box<dyn Error>> {
    let conn = Connection::open(path).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scans (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            service TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            data TEXT NOT NULL,
            version INTEGER NOT NULL,
            PRIMARY KEY (ip, port, service)
         )",
        [],
    )?;
    Ok(conn)
}

async fn setup_stream(config: ClientConfig) -> Result<MessageStream, Status> {
    let client = Client::new(config).await.unwrap();
    let subscription = client.subscription("scan-sub");
    Ok(subscription.subscribe(None).await?)
}

fn count_scans(conn: &Connection) -> Result<i64, Box<dyn Error>> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM scans",
        params![],
        |row| row.get(0),
    )?;
    Ok(count)
}

async fn run(config: ClientConfig) -> Result<(), Status> {
    let conn = setup_db(&Path::new("scan_store.db")).expect("Failed to setup database");
    let mut stream = setup_stream(config).await.expect("Failed to setup stream");
    
    for _ in 0..NUM_ITERS {
        if let Some(message) = stream.next().await {
            let res = process_message(&message.message.data, &conn);
            if res.is_err() {
                // retry/circuit break, dead letter queue, log to metrics, try to recover...
                message.nack().await?;
            } else {
                message.ack().await?;
            }
        }
    }

    println!("Done! {} unique records have been processed.", count_scans(&conn).unwrap());
    Ok(())
}

#[cfg(test)]
mod tests {
    // TESTS:
    // - updating old records
    // - handling both data versions
    // - handling duplicated
    // - handling out of order updates

    use super::*;
    
    fn setup_temp_db() -> Connection {
        // setup and clear db if needed
        let conn = setup_db(Path::new("test.db")).unwrap();
        conn.execute(
            "DELETE FROM scans",
            [],
        ).unwrap();
        conn
    }

    // returns all scans, ordered by ports for repeatability
    fn get_all_scans(conn: &Connection) -> Result<Vec<Scan>, Box<dyn Error>> {
        let mut stmt = conn.prepare("SELECT * FROM scans ORDER BY port;")?;
        let scans_iter = stmt.query_map([], |row| {
            Ok(Scan {
                ip: row.get(0)?,
                port: row.get(1)?,
                service: row.get(2)?,
                timestamp: row.get(3)?,
                data: Data::V2 { response_str: row.get(4)? },
                data_version: None,
                version: row.get(5)?
            })
        })?;

        let scans: Result<Vec<_>, _> = scans_iter.collect();
        scans.map_err(|e| e.into())
    }

    fn create_scan(ip: &str, port: u32, service: &str, timestamp: i64, data_version: i64, data_content: &str) -> Scan {
        Scan {
            ip: ip.to_string(),
            port,
            service: service.to_string(),
            timestamp,
            data: match data_version {
                1 => Data::V1 { response_bytes_utf8: general_purpose::STANDARD.encode(data_content.as_bytes()) },
                2 => Data::V2 { response_str: data_content.to_string() },
                _ => unreachable!()
            },
            data_version: Some(data_version),
            version: None
        }
    }

    fn assert_data_equals(scan: &Scan, expected: &str) {
        // for tests, get_all_scans only uses v2
        let data = match scan.data {
            Data::V2 { ref response_str } => response_str,
            Data::V1 {response_bytes_utf8: _} => unreachable!()
        };
        assert_eq!(data, expected);
    }

    #[test]
    fn test_overwrite() {
        let conn = setup_temp_db();
        let scan1_v1 = create_scan("127.0.0.1", 1, "FTP", 1, 1, "Test data");
        let scan2 = create_scan("127.0.0.1", 2, "SMTP", 1, 2, "Test data 2");
        let scan1_v2 = create_scan("127.0.0.1", 1, "FTP", 2, 2, "Test data updated");

        process_message(&serde_json::to_vec(&scan1_v1).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&scan2).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&scan1_v2).unwrap(), &conn).unwrap();

        let scans = get_all_scans(&conn).unwrap();
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].version, Some(2)); // port 1, version 2
        assert_eq!(scans[1].version, Some(1)); // port 2, version 1
        assert_data_equals(&scans[0], "Test data updated");
        assert_data_equals(&scans[1], "Test data 2");
    }

    #[test]
    fn test_both_versions() {
        let conn = setup_temp_db();
        let scan_v1 = create_scan("127.0.0.1", 1, "FTP", 1, 1, "Test data");
        let scan_v2 = create_scan("127.0.0.1", 2, "SMTP", 1, 2, "Test data 2");

        process_message(&serde_json::to_vec(&scan_v1).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&scan_v2).unwrap(), &conn).unwrap();

        let scans = get_all_scans(&conn).unwrap();
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].version, Some(1)); // port 1, version 1
        assert_eq!(scans[1].version, Some(1)); // port 1, version 1
        assert_data_equals(&scans[0], "Test data");
        assert_data_equals(&scans[1], "Test data 2");
    }
    
    #[tokio::test]
    async fn test_duplication() {
        let conn = setup_temp_db();
        let duplicated_scan = create_scan("127.0.0.1", 1, "HTTP", 1, 1, "Test data");
        let scan = create_scan("127.0.0.1", 2, "UDP", 1, 1, "Test data 2");

        process_message(&serde_json::to_vec(&duplicated_scan).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&scan).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&duplicated_scan).unwrap(), &conn).unwrap();


        let scans = get_all_scans(&conn).unwrap();

        // should be 2 unique hosts, on their first version
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].version, Some(1)); // port 1/duplicated, version should not be incremented
        assert_eq!(scans[1].version, Some(1)); // nonduplicated, port 2
        assert_data_equals(&scans[0], "Test data");
        assert_data_equals(&scans[1], "Test data 2");
    }
    #[test]
    fn test_out_of_order() {
        let conn = setup_temp_db();
        let inorder_scan1 = create_scan("127.0.0.1", 1, "HTTPS", 2, 1, "Test data");
        let inorder_scan2 = create_scan("127.0.0.1", 2, "UDP", 1, 1, "Test data 2");
    
        process_message(&serde_json::to_vec(&inorder_scan1).unwrap(), &conn).unwrap();
        process_message(&serde_json::to_vec(&inorder_scan2).unwrap(), &conn).unwrap();
    
        // make older
        let mut out_of_order_scan = inorder_scan1;
        out_of_order_scan.timestamp = 1;
        process_message(&serde_json::to_vec(&out_of_order_scan).unwrap(), &conn).unwrap();
    
        let scans = get_all_scans(&conn).unwrap();
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].version, Some(1)); // port 1, "in order / out of order"
        assert_eq!(scans[1].version, Some(1)); // port 2, "in order"
    }
}
