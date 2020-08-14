use std::{
    fs::{File, OpenOptions},
    io,
    ops::Deref,
    path::{Path, PathBuf},
};
pub struct RichFile {
    pub underlying: File,
    pub dir: String,
    pub name: String,
}
#[derive(Debug)]
pub enum FileOption {
    New,
    Append,
}
impl FileOption {
    fn open(&self, path: &PathBuf) -> Result<File, io::Error> {
        let mut option = OpenOptions::new();
        match self {
            FileOption::New => option.read(true).write(true).truncate(true).create(true),
            FileOption::Append => option.read(true).append(true).truncate(false).create(true),
        }
        .open(path)
    }
}
impl RichFile {
    pub fn open_file(
        dir_name: impl Into<String>,
        file_name: impl Into<String>,
        option: FileOption,
    ) -> Result<RichFile, io::Error> {
        let dir_name = dir_name.into();
        let dir = Path::new(&dir_name);
        let file_name_s: String = file_name.into();
        let path = dir.join(&file_name_s);
        let file = option
            .open(&path)
            .expect(format!("failed to open file({:?}), option: {:?}", &path, option).deref());

        Ok(RichFile {
            underlying: file,
            dir: dir_name,
            name: file_name_s,
        })
    }
    pub fn path(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.name)
    }
}
