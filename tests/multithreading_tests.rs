#[cfg(not(target_os = "wasi"))]
mod multithreading_test {
    use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
    use std::sync::Arc;
    use std::thread;

    fn create_tempfile() -> tempfile::NamedTempFile {
        if cfg!(target_os = "wasi") {
            tempfile::NamedTempFile::new_in("/tmp").unwrap()
        } else {
            tempfile::NamedTempFile::new().unwrap()
        }
    }

    const TABLE: TableDefinition<&str, &str> = TableDefinition::new("x");
    #[test]
    fn len() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let db = Arc::new(db);
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("hello", "world").unwrap();
            table.insert("hello2", "world2").unwrap();
            table.insert("hi", "world").unwrap();
        }
        write_txn.commit().unwrap();

        let db2 = db.clone();
        let t = thread::spawn(move || {
            let read_txn = db2.begin_read().unwrap();
            let table = read_txn.open_table(TABLE).unwrap();
            assert_eq!(table.len().unwrap(), 3);
        });
        t.join().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn multithreaded_insert() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    table2.insert("hello", "world").unwrap();
                    table2.insert("hello2", "world2").unwrap();
                });
            });

            table1.insert("hello", "world").unwrap();
            table1.insert("hello2", "world2").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 2);
    }

    #[test]
    fn multithreaded_re_read() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        const DEF3: TableDefinition<&str, &str> = TableDefinition::new("z");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();
            let mut table3 = write_txn.open_table(DEF3).unwrap();
            table1.insert("hello", "world").unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table2.insert("hello2", value.value()).unwrap();
                });
            });
            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table3.insert("hello2", value.value()).unwrap();
                });
            });

            assert_eq!(table2.get("hello2").unwrap().unwrap().value(), "world");
            assert_eq!(table3.get("hello2").unwrap().unwrap().value(), "world");
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF3).unwrap();
        assert_eq!(table.len().unwrap(), 1);
    }

    #[test]
    // Concurrent lookups on a single shared table, racing to fill the read
    // transaction's page cache, and on tables from separate transactions
    fn multithreaded_reads() {
        const TABLE: TableDefinition<u32, u64> = TableDefinition::new("multithreaded_reads");
        const ELEMENTS: u32 = 10_000;
        const THREADS: usize = 4;

        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            for i in 0..ELEMENTS {
                table.insert(i, u64::from(i) * 7).unwrap();
            }
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = Arc::new(read_txn.open_table(TABLE).unwrap());
        thread::scope(|s| {
            for _ in 0..THREADS {
                let table = table.clone();
                s.spawn(move || {
                    for i in 0..ELEMENTS {
                        assert_eq!(table.get(i).unwrap().unwrap().value(), u64::from(i) * 7);
                    }
                });
            }
        });

        thread::scope(|s| {
            for _ in 0..THREADS {
                let db = &db;
                s.spawn(move || {
                    let read_txn = db.begin_read().unwrap();
                    let table = read_txn.open_table(TABLE).unwrap();
                    for i in 0..ELEMENTS {
                        assert_eq!(table.get(i).unwrap().unwrap().value(), u64::from(i) * 7);
                    }
                });
            }
        });
    }

    #[test]
    // Readers fill their transactions' page caches while a writer concurrently rewrites
    // the whole tree. Each read snapshot must stay internally consistent: every lookup
    // in one transaction must observe the same round of writes.
    fn concurrent_reads_and_writes() {
        const TABLE: TableDefinition<u32, &[u8]> = TableDefinition::new("reads_and_writes");
        const ELEMENTS: u32 = 20_000;
        const ROUNDS: u8 = 4;
        const READERS: usize = 3;

        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            for i in 0..ELEMENTS {
                table.insert(i, [0u8; 50].as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();

        thread::scope(|s| {
            s.spawn(|| {
                for round in 1..=ROUNDS {
                    let write_txn = db.begin_write().unwrap();
                    {
                        let mut table = write_txn.open_table(TABLE).unwrap();
                        for i in 0..ELEMENTS {
                            table.insert(i, [round; 50].as_slice()).unwrap();
                        }
                    }
                    write_txn.commit().unwrap();
                }
            });
            for _ in 0..READERS {
                s.spawn(|| {
                    for _ in 0..2 {
                        let read_txn = db.begin_read().unwrap();
                        let table = read_txn.open_table(TABLE).unwrap();
                        let round = table.get(0).unwrap().unwrap().value()[0];
                        for i in 0..ELEMENTS {
                            assert_eq!(table.get(i).unwrap().unwrap().value(), [round; 50]);
                        }
                    }
                });
            }
        });

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        for i in 0..ELEMENTS {
            assert_eq!(table.get(i).unwrap().unwrap().value(), [ROUNDS; 50]);
        }
    }
}
