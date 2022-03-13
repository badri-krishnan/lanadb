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
    //Create New WAL
    pub fn new(dir: &Path) -> io::Result<WAL>{
        let timestamp = SystemTime::now().
            duration_since(UNIX_EPOCH).
            unwrap().as_micros();
        let wal_path = Path::new(dir).join(timestamp.to_string()+".wal");
        let wal_file = OpenOptions::new().append(true).create(true).open(&wal_path)?;
        let wal_file = BufWriter::new(wal_file);

        Ok(WAL{wal_path, wal_file})
    }

    //Create WAL from path
    pub fn from_path(path: &Path) -> io::Result<WAL>{
        let wal_file = OpenOptions::new().append(true).create(true).open(&path)?;
        let wal_file = BufWriter::new(wal_file);
        let wal_path = path.to_owned();
        Ok(WAL{
            wal_path, 
            wal_file
        })
    }
    
    //Set Records in the WAL
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
    
    //Delete Record in the WAL
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

#[cfg(test)]
mod tests {
    use crate::wal::{WAL, self};
    use rand::Rng;
    use std::fs::{create_dir, remove_dir_all};
    use std::fs::{metadata, File, OpenOptions};
    use std::io::prelude::*;
    use std::io::BufReader;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    
    //Helper method to validate WAL Record Block Format and Value
    fn validate_wal_record(reader: &mut BufReader<File>,key: &[u8], value: Option<&[u8]>,timestamp: u128,deleted: bool){
        let mut len_buffer = [0;8];
        reader.read_exact(&mut len_buffer).unwrap();
        
        //Read first buffer 
        let wal_key_len = usize::from_le_bytes(len_buffer);
        assert_eq!(wal_key_len, key.len());

        let mut tombstone_buf = [0;1];
        reader.read_exact(&mut tombstone_buf).unwrap();
        let record_deleted = tombstone_buf[0] != 0;
        println!("record deleted value : {}", record_deleted);
        assert_eq!(record_deleted, deleted);

        if deleted {
            let mut wal_key = vec![0;wal_key_len];
            reader.read_exact(&mut wal_key).unwrap();
            assert_eq!(wal_key, key);
        } else {
            reader.read_exact(&mut len_buffer).unwrap();
            let wal_value_len = usize::from_le_bytes(len_buffer);
            assert_eq!(wal_value_len, value.unwrap().len());

            let mut wal_key = vec![0;wal_key_len];
            reader.read_exact(&mut wal_key).unwrap();
            assert_eq!(wal_key, key);

            let mut wal_value = vec![0;wal_value_len];
            reader.read_exact(&mut wal_value).unwrap();
            assert_eq!(wal_value, value.unwrap())
        }
        let mut timestamp_buffer = [0; 16];
        reader.read_exact(&mut timestamp_buffer).unwrap();
        let wal_timestamp = u128::from_le_bytes(timestamp_buffer);
        assert_eq!(wal_timestamp, timestamp);
    }

    #[test]
    fn test_write_one() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();
    
        let timestamp = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_micros();
    
        let mut wal = WAL::new(&dir).unwrap();
        wal.set(b"Badri", b"Badri Krishnan", timestamp).unwrap();
        wal.flush().unwrap();
    
        let wal_file = OpenOptions::new().read(true).open(&wal.wal_path).unwrap();
        let mut reader = BufReader::new(wal_file);
    
        validate_wal_record(
          &mut reader,
          b"Badri",
          Some(b"Badri Krishnan"),
          timestamp,
          false,
        );
    
        remove_dir_all(&dir).unwrap();
    }
    
    #[test]
    fn test_write_many_records() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        println!("HEllo : {}",dir.to_str().unwrap());
        create_dir(&dir).unwrap();

        let timestamp = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_micros();
        
        let records: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Car", Some(b"Garage")),
            (b"Bike", Some(b"Bike Rack")),
            (b"Pedestrian", Some(b"Pedastrian Walkway")),
        ];
        let mut wal = WAL::new(&dir).unwrap();
        for record in records.iter(){
            wal.set(record.0, record.1.unwrap(), timestamp);
        }
        wal.flush().unwrap();
        let wal_file = OpenOptions::new().read(true).open(&wal.wal_path).unwrap();
        let mut reader = BufReader::new(wal_file);
        for record in records.iter(){
            validate_wal_record(&mut reader, record.0, record.1, timestamp, false);
        }

        remove_dir_all(&dir).unwrap();
    }
    #[test]
    fn test_write_and_delete() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        println!("HEllo : {}",dir.to_str().unwrap());
        create_dir(&dir).unwrap();

        let timestamp = SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_micros();
        
        let records: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Car", Some(b"Garage")),
            (b"Bike", Some(b"Bike Rack")),
            (b"Pedestrian", Some(b"Pedastrian Walkway")),
        ];
        let mut wal = WAL::new(&dir).unwrap();
        for record in records.iter(){
            wal.set(record.0, record.1.unwrap(), timestamp);
        }
        for record in records.iter(){
            wal.delete(record.0, timestamp);
        }
        wal.flush().unwrap();
        let wal_file = OpenOptions::new().read(true).open(&wal.wal_path).unwrap();
        let mut reader = BufReader::new(wal_file);
        for record in records.iter() {
            validate_wal_record(&mut reader, record.0, record.1, timestamp, false);
        }
        for record in records.iter() {
            validate_wal_record(&mut reader, record.0, None, timestamp, true);
        }
        remove_dir_all(&dir).unwrap();
    }
    #[test]
    fn test_read_wal_none() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let (new_wal, new_mem_table) = WAL::load_mem_table_from_dir(&dir).unwrap();
        assert_eq!(new_mem_table.len(), 0);

        let m = metadata(new_wal.wal_path).unwrap();
        assert_eq!(m.len(), 0);

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_read_wal_one(){
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();    

        let records: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Car", Some(b"Garage")),
            (b"Bike", Some(b"Bike Rack")),
            (b"Pedestrian", Some(b"Pedastrian Walkway")),
        ];
        let mut wal = WAL::new(&dir).unwrap();
        for (time, record) in records.iter().enumerate(){
            wal.set(record.0,record.1.unwrap(), time as u128).unwrap();
        }
        wal.flush().unwrap();
        let (new_wal, mut recovered_table) = WAL::load_mem_table_from_dir(&dir).unwrap();
        
        let file = OpenOptions::new().read(true).open(&new_wal.wal_path).unwrap();
        let mut reader = BufReader::new(file);

        for (time, record) in records.iter().enumerate(){
            validate_wal_record(&mut reader, record.0, record.1, time as u128, false);
            let record_from_mem_table = recovered_table.get(record.0).unwrap();
            assert_eq!(record_from_mem_table.key, record.0);
            assert_eq!(record_from_mem_table.value.as_ref().unwrap().as_slice(), record.1.unwrap());
            assert_eq!(record_from_mem_table.timestamp, time as u128);
        }

        remove_dir_all(&dir).unwrap();

    }
}