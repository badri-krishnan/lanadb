use std::fs::{File,OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::path::PathBuf;


pub struct WALRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool
}

pub struct WALRecordIterator {
    buffered_reader: BufReader<File>,
}
impl WALRecordIterator {
    pub fn new(path: PathBuf) -> io::Result<WALRecordIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let buff_reader = BufReader::new(file);
        Ok(WALRecordIterator{
            buffered_reader: buff_reader
        })
    }
}

impl Iterator for WALRecordIterator{
    type Item = WALRecord;

    fn next(&mut self) -> Option<WALRecord>{
        let mut len_buffer = [0;8];
        if self.buffered_reader.read_exact(& mut len_buffer).is_err(){
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);
        
        let mut tombstone_buffer = [0;1];

        if self.buffered_reader.read_exact(& mut tombstone_buffer).is_err(){
            return None;
        }

        let deleted = tombstone_buffer[0] != 0;
        let mut key = vec![0 ; key_len];
        let mut value = None;
        if (deleted){
            if self.buffered_reader.read_exact(& mut key).is_err() {
                return None;
            }  
        } 
        else {
            if self.buffered_reader.read_exact(&mut len_buffer).is_err() {
              return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.buffered_reader.read_exact(&mut key).is_err() {
              return None;
            }
            let mut value_buffer = vec![0; value_len];
            if self.buffered_reader.read_exact(&mut value_buffer).is_err() {
              return None;
            }
            value = Some(value_buffer);
        }

        let mut timestamp_buffer = [0; 16];
        if self.buffered_reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);
        
        Some(
            WALRecord{
                key,
                value,
                timestamp,
                deleted
            }
        )
    }
}
