#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use rusqlite::Connection;
use sync_engine::{CrrTable, SqliteStorage, MemoryStorage, Changeset};

pub fn generate_file_record(idx: usize) -> HashMap<String, String> {
    let dirs = ["Documents", "Pictures", "Videos", "Downloads", "Projects"];
    let exts = ["pdf", "jpg", "mp4", "txt", "rs"];
    let owners = ["alice", "bob", "carol", "david"];

    let mut record = HashMap::new();
    record.insert("path".into(), format!("/{}/file_{}.{}", dirs[idx % 5], idx, exts[idx % 5]));
    record.insert("filename".into(), format!("file_{}.{}", idx, exts[idx % 5]));
    record.insert("extension".into(), exts[idx % 5].into());
    record.insert("size_bytes".into(), (1000 + idx * 100).to_string());
    record.insert("checksum".into(), format!("{:064x}", idx as u128 * 0xdeadbeef));
    record.insert("mime_type".into(), "application/octet-stream".into());
    record.insert("owner".into(), owners[idx % 4].into());
    record.insert("created_at".into(), (1704067200 + idx as u64).to_string());
    record.insert("modified_at".into(), (1704067200 + idx as u64 + 3600).to_string());
    record.insert("inode".into(), (100000 + idx).to_string());
    record.insert("permissions".into(), "644".into());
    record.insert("tags".into(), "work,important".into());
    record
}

pub fn create_table(rows: usize) -> CrrTable {
    let mut table = CrrTable::open_in_memory().unwrap();
    for i in 0..rows {
        let record = generate_file_record(i);
        let pk = format!("file_{}", i);
        let mut builder = table.insert(&pk);
        for (col, value) in &record {
            builder = builder.column_str(col, value, 1);
        }
        builder.commit().unwrap();
    }
    table
}

pub fn create_changeset(count: usize, version: u64) -> Changeset {
    let mut changes = HashMap::new();
    for i in 0..count {
        let pk = format!("file_{}", i);
        let mut record = generate_file_record(i);
        record.insert("modified_at".into(), (1704067200 + i as u64 + 86400).to_string());
        record.insert("tags".into(), "updated,synced".into());

        let columns: HashMap<String, Vec<u8>> = record
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect();
        let versions: HashMap<String, u64> = columns.keys()
            .map(|k| (k.clone(), version))
            .collect();
        changes.insert(pk, (columns, versions));
    }
    Changeset { changes }
}

pub fn create_table_sqlite(rows: usize) -> CrrTable<SqliteStorage> {
    let storage = SqliteStorage::open_in_memory().unwrap();
    let mut table = CrrTable::with_storage(storage);
    for i in 0..rows {
        let record = generate_file_record(i);
        let pk = format!("file_{}", i);
        let mut builder = table.insert(&pk);
        for (col, value) in &record {
            builder = builder.column_str(col, value, 1);
        }
        builder.commit().unwrap();
    }
    table
}

pub fn create_table_memory(rows: usize) -> CrrTable<MemoryStorage> {
    let storage = MemoryStorage::new();
    let mut table = CrrTable::with_storage(storage);
    for i in 0..rows {
        let record = generate_file_record(i);
        let pk = format!("file_{}", i);
        let mut builder = table.insert(&pk);
        for (col, value) in &record {
            builder = builder.column_str(col, value, 1);
        }
        builder.commit().unwrap();
    }
    table
}

pub fn create_changeset_for_sqlite(count: usize, version: u64) -> Changeset {
    let mut changes = HashMap::new();
    for i in 0..count {
        let pk = format!("file_{}", i);
        let mut record = generate_file_record(i);
        record.insert("modified_at".into(), (1704067200 + i as u64 + 86400).to_string());
        record.insert("tags".into(), "updated,synced".into());

        let columns: HashMap<String, Vec<u8>> = record
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect();
        let versions: HashMap<String, u64> = columns.keys()
            .map(|k| (k.clone(), version))
            .collect();
        changes.insert(pk, (columns, versions));
    }
    Changeset { changes }
}

#[allow(dead_code)]
const CRSQLITE_PATHS: &[&str] = &[
    "./vendor/crsqlite.dylib",
    "./vendor/crsqlite.so",
    "./vendor/crsqlite",
];

#[allow(dead_code)]
pub fn load_crsqlite(conn: &Connection) -> bool {
    unsafe {
        if conn.load_extension_enable().is_err() {
            return false;
        }
    }

    for path in CRSQLITE_PATHS {
        if Path::new(path).exists() {
            unsafe {
                if conn.load_extension(path, None).is_ok() {
                    let _ = conn.load_extension_disable();
                    return true;
                }
            }
        }
    }

    let _ = conn.load_extension_disable();
    false
}

#[allow(dead_code)]
pub fn setup_crsqlite_table(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY NOT NULL,
            path TEXT, filename TEXT, extension TEXT, size_bytes INTEGER,
            checksum TEXT, mime_type TEXT, owner TEXT, created_at INTEGER,
            modified_at INTEGER, inode INTEGER, permissions TEXT, tags TEXT
        )", [],
    ).unwrap();

    let _ = conn.prepare("SELECT crsql_as_crr('files')").unwrap().query([]).unwrap();
}

#[allow(dead_code)]
pub fn crsqlite_insert(conn: &Connection, count: usize) {
    let mut stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO files
         (id, path, filename, extension, size_bytes, checksum, mime_type,
          owner, created_at, modified_at, inode, permissions, tags)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
    ).unwrap();

    for i in 0..count {
        let r = generate_file_record(i);
        stmt.execute(rusqlite::params![
            format!("file_{}", i),
            r["path"], r["filename"], r["extension"],
            r["size_bytes"].parse::<i64>().unwrap(),
            r["checksum"], r["mime_type"], r["owner"],
            r["created_at"].parse::<i64>().unwrap(),
            r["modified_at"].parse::<i64>().unwrap(),
            r["inode"].parse::<i64>().unwrap(),
            r["permissions"], r["tags"],
        ]).unwrap();
    }
}
