//This memtable will hold a sorted list of key-value records
//We will write a duplicate to the WAL in case a failure in lanadb
//There will be a max capacity to a MemTable at which point we will flust the table to the Disk
//Entries will be stored in a HashMap 
use std::ops::Index;

pub struct MemTable{
    entries: Vec<Record>,
    size: usize,
}

pub struct Record{
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool
}

impl MemTable{
    pub fn new() -> MemTable{
        MemTable{
            entries: Vec::new(),
            size: 0,
        }
    }
    pub fn set(&mut self, key:&[u8], value:&[u8], timestamp:u128) {
        let entry = Record {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false
        };
        //Binary search helps us find the index that entry can be stored
        //this is so we always maintain the sorted list structure for O(log n) look up
        match self.get_index(key){
            Ok(idx) => {
                println!("this is the index for this key in the entries vector: {}",idx);
                //if there was key-val pair before we are finding the difference in the size of the value 
                //Then we update the size of Memtable based on the difference and setting the entry
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                      self.size -= v.len() - value.len();
                    } else {
                      self.size += value.len() - v.len();
                    }
                  }
                self.entries[idx] = entry;
            }
            //Key does not exist so we are adding new entry and update new size for the Memtable
            Err(idx) => {
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(idx,entry);
            }
        }
    }
    //Delete record from the Memtable
    pub fn delete(&mut self, key: &[u8], timestamp: u128){
        let entry = Record{
            key: key.to_owned(),
            value: None,
            timestamp: timestamp,
            deleted: true
        };

        match self.get_index(key) {
            Ok(idx) => {
                if let Some(value) = self.entries[idx].value.as_ref(){
                    self.size -= value.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + 16 + 1;
                self.entries.insert(idx, entry);  
            }
        }
    }
    pub fn get(&mut self, key: &[u8]) -> Option<&Record>{
        if let Ok(index) = self.get_index(key){
            return Some(&self.entries[index]);
        }
        else {
            return None;
        }
    }
     // # of records in the MemTable.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    //all of the records from the MemTable.
    pub fn entries(&self) -> &[Record] {
        &self.entries
    }

    //total size of the records in the MemTable
    pub fn size(&self) -> usize {
        self.size
    }


    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&key, |e| e.key.as_slice())
    }

}

#[cfg(test)]
mod tests {
    use crate::mem_table::MemTable;
  
    #[test]
    fn test_mem_table_put_start() {
      let mut table = MemTable::new();
      table.set(b"Badri", b"Badri Krishnan", 10); // 5 + 14 + 16 + 1= 36
      table.set(b"Lavanya", b"Lavanya Krishnan", 20); // 7 + 16 + 16 + 1 = 40
  
      table.set(b"Keerthi", b"Keerthi Krishnan", 0); // 7 + 16 + 16 + 1 = 40
  
      assert_eq!(table.entries[0].key, b"Badri");
      assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Badri Krishnan");
      assert_eq!(table.entries[0].timestamp, 10);
      assert_eq!(table.entries[0].deleted, false);
      assert_eq!(table.entries[1].key, b"Keerthi");
      assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Keerthi Krishnan");
      assert_eq!(table.entries[1].timestamp, 0);
      assert_eq!(table.entries[1].deleted, false);
      assert_eq!(table.entries[2].key, b"Lavanya");
      assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Lavanya Krishnan");
      assert_eq!(table.entries[2].timestamp, 20);
      assert_eq!(table.entries[2].deleted, false);
  
      assert_eq!(table.size, 116);
    }
  
    #[test]
    fn test_mem_table_set_middle() {
      let mut table = MemTable::new();
      table.set(b"Badri", b"Badri Krishnan", 10); // 5 + 14 + 16 + 1= 36
      table.set(b"Lavanya", b"Lavanya Krishnan", 20); // 7 + 16 + 16 + 1 = 40
  
      table.set(b"Keerthi", b"Keerthi Krishnan", 0); // 7 + 16 + 16 + 1 = 40
  
      table.set(b"Car", b"Car Krishnan", 30); //3 + 12 + 16 + 1 = 32
  
      assert_eq!(table.entries[0].key, b"Badri");
      assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Badri Krishnan");
      assert_eq!(table.entries[0].timestamp, 10);
      assert_eq!(table.entries[0].deleted, false);
      assert_eq!(table.entries[1].key, b"Car");
      assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Car Krishnan");
      assert_eq!(table.entries[1].timestamp, 30);
      assert_eq!(table.entries[1].deleted, false);
      assert_eq!(table.entries[2].key, b"Keerthi");
      assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Keerthi Krishnan");
      assert_eq!(table.entries[2].timestamp, 0);
      assert_eq!(table.entries[2].deleted, false);
  
      assert_eq!(table.size, 148);
    }
  
    #[test]
    fn test_mem_table_put_end() {
      let mut table = MemTable::new();
      table.set(b"Apple", b"Apple Smoothie", 0);
      table.set(b"Lime", b"Lime Smoothie", 10);
  
      table.set(b"Orange", b"Orange Smoothie", 20);
  
      assert_eq!(table.entries[0].key, b"Apple");
      assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
      assert_eq!(table.entries[0].timestamp, 0);
      assert_eq!(table.entries[0].deleted, false);
      assert_eq!(table.entries[1].key, b"Lime");
      assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
      assert_eq!(table.entries[1].timestamp, 10);
      assert_eq!(table.entries[1].deleted, false);
      assert_eq!(table.entries[2].key, b"Orange");
      assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
      assert_eq!(table.entries[2].timestamp, 20);
      assert_eq!(table.entries[2].deleted, false);
  
      assert_eq!(table.size, 108);
    }
  
    #[test]
    fn test_mem_table_put_overwrite() {
      let mut table = MemTable::new();
      table.set(b"Apple", b"Apple Smoothie", 0);
      table.set(b"Lime", b"Lime Smoothie", 10);
      table.set(b"Orange", b"Orange Smoothie", 20);
  
      table.set(b"Lime", b"A sour fruit", 30);
  
      assert_eq!(table.entries[0].key, b"Apple");
      assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
      assert_eq!(table.entries[0].timestamp, 0);
      assert_eq!(table.entries[0].deleted, false);
      assert_eq!(table.entries[1].key, b"Lime");
      assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
      assert_eq!(table.entries[1].timestamp, 30);
      assert_eq!(table.entries[1].deleted, false);
      assert_eq!(table.entries[2].key, b"Orange");
      assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
      assert_eq!(table.entries[2].timestamp, 20);
      assert_eq!(table.entries[2].deleted, false);
  
      assert_eq!(table.size, 107);
    }
  
    #[test]
    fn test_mem_table_get_exists() {
      let mut table = MemTable::new();
      table.set(b"Apple", b"Apple Smoothie", 0);
      table.set(b"Lime", b"Lime Smoothie", 10);
      table.set(b"Orange", b"Orange Smoothie", 20);
  
      let entry = table.get(b"Orange").unwrap();
  
      assert_eq!(entry.key, b"Orange");
      assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
      assert_eq!(entry.timestamp, 20);
    }
  
    #[test]
    fn test_mem_table_get_not_exists() {
      let mut table = MemTable::new();
      table.set(b"Apple", b"Apple Smoothie", 0);
      table.set(b"Lime", b"Lime Smoothie", 0);
      table.set(b"Orange", b"Orange Smoothie", 0);
  
      let res = table.get(b"Potato");
      assert_eq!(res.is_some(), false);
    }
  
    #[test]
    fn test_mem_table_delete_exists() {
      let mut table = MemTable::new();
      table.set(b"Apple", b"Apple Smoothie", 0);
  
      table.delete(b"Apple", 10);
  
      let res = table.get(b"Apple").unwrap();
      assert_eq!(res.key, b"Apple");
      assert_eq!(res.value, None);
      assert_eq!(res.timestamp, 10);
      assert_eq!(res.deleted, true);
  
      assert_eq!(table.entries[0].key, b"Apple");
      assert_eq!(table.entries[0].value, None);
      assert_eq!(table.entries[0].timestamp, 10);
      assert_eq!(table.entries[0].deleted, true);
  
      assert_eq!(table.size, 22);
    }
  
    #[test]
    fn test_mem_table_delete_empty() {
      let mut table = MemTable::new();
  
      table.delete(b"Apple", 10);
  
      let res = table.get(b"Apple").unwrap();
      assert_eq!(res.key, b"Apple");
      assert_eq!(res.value, None);
      assert_eq!(res.timestamp, 10);
      assert_eq!(res.deleted, true);
  
      assert_eq!(table.entries[0].key, b"Apple");
      assert_eq!(table.entries[0].value, None);
      assert_eq!(table.entries[0].timestamp, 10);
      assert_eq!(table.entries[0].deleted, true);
  
      assert_eq!(table.size, 22);
    }
  }