use crate::sst::rich_file::*;
use io::{BufRead, BufReader, Lines, Write};
use std::{
    fs::File,
    io::{self, BufWriter},
};

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
    type Item = Result<Entry, String>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.buf.next() {
            Some(Ok(line)) => Some(WriteAheadLog::parse_line(&line)),
            _ => None,
        }
    }
}

impl WriteAheadLog {
    const FILE_NAME: &'static str = "wal.log";
    const DELIMITER: &'static str = "\0";
    const TAG_DELETED: &'static str = "D";
    const TAG_INSERTED: &'static str = "I";

    pub fn create(dir_name: &str) -> WriteAheadLog {
        WriteAheadLog {
            dir_name: dir_name.into(),
            writer: Self::writer(dir_name),
        }
    }
    fn writer(dir_name: &str) -> BufWriter<File> {
      let file = Self::open_file(dir_name, FileOption::New).expect("failed to open WAL file");
      BufWriter::new(file.underlying)
    }
    fn open_file(dir_name: &str, option: FileOption) -> io::Result<RichFile> {
        RichFile::open_file(dir_name, Self::FILE_NAME, option)
    }

    pub fn insert(&mut self, entry: (&str, &str)) -> io::Result<()> {
        let (key, value) = entry;
        let str = format!(
            "{}{}{}{}{}\n",
            Self::TAG_INSERTED,
            Self::DELIMITER,
            key,
            Self::DELIMITER,
            value
        );
        self.writer.write(str.as_bytes());
        self.writer.flush()
    }

    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        let str = format!("{}{}{}\n", Self::TAG_DELETED, Self::DELIMITER, key);
        self.writer.write(str.as_bytes());
        self.writer.flush()
    }

    pub fn clear(&mut self) -> io::Result<()> {
      self.writer = Self::writer(&self.dir_name);
      Ok(())
    }

    pub fn restore(dir_name: &str) -> Option<WalRestore> {
        match Self::open_file(dir_name, FileOption::ReadOnly) {
            Ok(file) => {
                let buf = BufReader::new(file.underlying).lines();
                Some(WalRestore { buf })
            }
            _ => None,
        }
    }
    fn parse_line(line: &str) -> Result<Entry, String> {
        let res: Vec<_> = line.split(Self::DELIMITER).collect();
        match res[0] {
            Self::TAG_INSERTED => {
                let key = res[1].to_string();
                let value = res[2].to_string();
                Ok(Entry::Inserted { key, value })
            }
            Self::TAG_DELETED => {
                let key = res[1].to_string();
                Ok(Entry::Deleted { key })
            }
            _ => Err(format!(
                "unknown tag({}) was written in line({:?})",
                res[0], res
            )),
        }
    }
}
