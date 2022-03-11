//WAL - Write Ahead Log

/*
This an append only log to recover the Database in case of server shutdown
It Holds the operation performed on the DB, has the file structured in the following record format

+---------------+---------------+-----------------+-...-+--...--+-----------------+
| Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key(Variable) | Value(Variable) | Timestamp (16B) |
+---------------+---------------+-----------------+-...-+--...--+-----------------+
Key Size = Length of the Key data
Tombstone = If this record was deleted and has a value
Value Size = Length of the Value data
Key = Key data
Value = Value data
Timestamp = Timestamp of the operation in microseconds

Each entry in DB will have one WAL record buffer associated to it

*/

use std::fs::{File,OpenOptions,remove_file};
use std::io::prelude::*;
use std::io::{self, BufWriter};
use std::path::{Path,PathBuf};
use std::time::{SystemTime,UNIX_EPOCH};

use crate::mem_table::MemTable;
use crate::utils::files_with_ext;
use crate::wal_iterator::{WALRecordIterator, WALRecord};

pub struct WAL{
    wal_path: PathBuf,
    wal_file: BufWriter<File>,
}

impl WAL{
    pub fn new(dir: &Path) -> io::Result<WAL>{
        let timestamp = SystemTime::now().
            duration_since(UNIX_EPOCH).
            unwrap().as_micros();
        let wal_path = Path::new(dir).join(timestamp.to_string()+".wal");
        let wal_file = OpenOptions::new().append(true).create(true).open(&wal_path)?;
        let wal_file = BufWriter::new(wal_file);

        Ok(WAL{wal_path, wal_file})
    }
    pub fn from_path(path: &Path) -> io::Result<WAL>{
        let wal_file = OpenOptions::new().append(true).create(true).open(&path)?;
        let wal_file = BufWriter::new(wal_file);
        let wal_path = path.to_owned();
        Ok(WAL{
            wal_path, 
            wal_file
        })
    }

    pub fn set(&mut self, key:&[u8], value:&[u8], timestamp:u128) ->io::Result<()>{
        //Key size write buffer
        self.wal_file.write_all(&key.len().to_le_bytes())?;
        
        //tombstone write buffer
        let tombstone = false as u8;
        self.wal_file.write_all(&tombstone.to_le_bytes())?;
        
        //value size write buffer
        self.wal_file.write_all(&value.len().to_le_bytes())?;

        //Write the Key & Value & TimeStamp
        self.wal_file.write_all(key)?;
        self.wal_file.write_all(value)?;
        self.wal_file.write_all(&timestamp.to_le_bytes())?;
        Ok(())

    }
    pub fn delete(&mut self, key:&[u8], timestamp:u128) -> io::Result<()>{
        //Key size write buffer
        self.wal_file.write_all(&key.len().to_le_bytes())?;
        
        //tombstone write buffer - deleted = true
        let tombstone = true as u8;
        self.wal_file.write_all(&tombstone.to_le_bytes())?;

        //Key & timestamp write only
        self.wal_file.write_all(key)?;
        self.wal_file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()>{
        self.wal_file.flush()
    }

    pub fn load_mem_table_from_dir(dir:&Path) -> io::Result<(WAL,MemTable)>{
        let extension = "wal";
        
        let mut wal_files = files_with_ext(dir, extension);
        
        //Multiple WAL in path then sort by date
        wal_files.sort();

        let mut recovery_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir)?;

        for file in wal_files.iter(){
            if let Ok(current_wal) = WAL::from_path(file){
                for wal_record in current_wal.into_iter(){
                    if wal_record.deleted {
                        recovery_mem_table.delete(wal_record.key.as_slice(), wal_record.timestamp);
                        new_wal.delete(wal_record.key.as_slice(), wal_record.timestamp)?;
                    } 
                    else{
                        recovery_mem_table.set(
                            wal_record.key.as_slice(),
                            wal_record.value.as_ref().unwrap().as_slice(),
                            wal_record.timestamp
                        );
                        new_wal.set(
                            wal_record.key.as_slice(),
                            wal_record.value.unwrap().as_slice(),
                            wal_record.timestamp
                        )?;
                    }
                }
            }
        }
        //write buffer to file
        new_wal.flush().unwrap();
        
        //Delete previous wal files
        wal_files.into_iter().for_each(|wf| remove_file(wf).unwrap());
        Ok((new_wal, recovery_mem_table))
    }

}

impl IntoIterator for WAL {
    type IntoIter = WALRecordIterator;
    type Item = WALRecord;
  
    /// Converts a WAL into a `WALIterator` to iterate over the entries.
    fn into_iter(self) -> WALRecordIterator {
        WALRecordIterator::new(self.wal_path).unwrap()
    }
  }