use crate::sst::rich_file::*;
use std::{fs::File, io::{self, BufWriter}};
use io::{BufReader, Write, BufRead, Lines};

pub(crate) enum Entry {
  Inserted { key: String, value: String },
  Deleted { key: String },
}

pub(crate) struct WriteAheadLog {
  dir_name: String,
  writer: BufWriter<File>,
}
pub(crate) struct WalRestore {
  buf: Lines<BufReader<File>>,
}
impl Iterator for WalRestore {
    type Item = Entry;
    fn next(&mut self) -> Option<Self::Item> {
      match self.buf.next() {
        Some(Ok(line)) => Some(WriteAheadLog::parse_line(&line)),
        _ => None,
      }
    } 
}

impl WriteAheadLog {
  const FILE_NAME: &'static str = "wal.log";
  const DELIMITER: &'static str = "\t";
  const TAG_DELETED: &'static str = "D";
  const TAG_INSERTED: &'static str = "I";

  pub fn new(dir_name: &str) -> WriteAheadLog { 
    let file = Self::open_file(dir_name).expect("failed to open WAL file");
    let writer = BufWriter::new(file.underlying);
    WriteAheadLog {
      dir_name: dir_name.into(),
      writer,
    }
  }
  fn open_file(dir_name: &str) -> io::Result<RichFile> {
    RichFile::open_file(dir_name, Self::FILE_NAME, FileOption::New)
  }

  pub fn insert(&mut self, entry: (&str, &str)) -> io::Result<()> { 
    let (key, value) = entry;
    let str = format!("{}{}{}{}{}\n", Self::TAG_INSERTED, Self::DELIMITER, key, Self::DELIMITER, value);
    self.writer.write(str.as_bytes());
    self.writer.flush()
  }

  pub fn delete(&mut self, key: &str) -> io::Result<()> { 
    let str = format!("{}{}{}\n", Self::TAG_DELETED, Self::DELIMITER, key);
    self.writer.write(str.as_bytes());
    self.writer.flush()
  }

  pub fn clear(&self) -> io::Result<()> { 
    let file = Self::open_file(&self.dir_name).expect("failed to open WAL file");
    file.underlying.set_len(0)?; // https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.set_len
    Ok(())
  }

  pub fn restore(dir_name: &str) -> io::Result<WalRestore> { 
    let file = Self::open_file(dir_name)?;
    let buf = BufReader::new(file.underlying).lines();
    Ok(WalRestore { buf })
  }
  fn parse_line(line: &str) -> Entry {
    let res: Vec<_> = line.split(Self::DELIMITER).collect();
    let tag = res[0].to_string();
    if tag == Self::TAG_INSERTED {
        let key = res[1].to_string();
        let value = res[2].to_string();
        Entry::Inserted { key, value }
    } else if tag == Self::TAG_DELETED {
        let key = res[1].to_string();
        Entry::Deleted { key }
    } else {
      unreachable!("unknown tag({}) was written.", tag);
    }
  }
}