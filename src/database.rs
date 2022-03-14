use std::path::{PathBuf, Path, self};
use crate::mem_table::MemTable;
use crate::wal::WAL;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DatabaseRecord{
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u128
}

impl DatabaseRecord {
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    
    pub fn timestamp(&self) -> u128 {
        self.timestamp
    }
}

pub struct Database{
    dir: PathBuf,
    mem_table: MemTable,
    wal: WAL,
}

impl Database{
    pub fn new(dir:&str) -> Database{
        let dir_buffer = PathBuf::from(dir);
        let path_dir = Path::new(dir);

        let (wal, mem_table) = WAL::load_mem_table_from_dir(&path_dir).unwrap();

        Database{
            dir: dir_buffer,
            wal: wal,
            mem_table: mem_table
        }
    }

    pub fn get(&mut self, key:&[u8]) -> Option<DatabaseRecord>{
        let record = self.mem_table.get(key);
        if record.is_some(){
           return Some(DatabaseRecord{
                key: record.unwrap().key.clone(),
                value: record.unwrap().value.as_ref().unwrap().clone(),
                timestamp: record.unwrap().timestamp,
            })
        }
        None
    }
        

    pub fn set(&mut self, key:&[u8], value:&[u8]) -> Result<usize, usize>{
        let timestamp = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_micros();
        let wal_result = self.wal.set(key, value, timestamp);
        
        if wal_result.is_err(){
            return Err(0);
        }

        if self.wal.flush().is_err(){
            return Err(0);
        }
        
        self.mem_table.set(key, value,timestamp);
        Ok(1)
    }
    pub fn delete(&mut self, key:&[u8]) -> Result<usize, usize> {
        let timestamp = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_micros();
        
        let wal_result = self.wal.delete(key,timestamp);
        if wal_result.is_err(){
            return Err(0);
        }

        if self.wal.flush().is_err(){
            return Err(0);
        }
        self.mem_table.delete(key, timestamp);
        Ok(1)
    }
}
