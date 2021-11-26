use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use libutils::tokio;
use libutils::tokio::fs::{File, OpenOptions};

/// The representation of SDFS's flat filesystem
pub struct FileSystem {
    base_path: OsString,
    files: HashSet<PathBuf>,
}

impl FileSystem {
    pub fn new(base_path: OsString) -> Self {
        Self {
            base_path,
            files: HashSet::new(),
        }
    }

    fn file_path(&self, file_name: &Path) -> PathBuf {
        Path::new(&self.base_path).join(file_name)
    }

    pub async fn list_files(&self) -> &HashSet<PathBuf> {
        return &self.files;
    }

    pub async fn insert<P: AsRef<Path>>(&mut self, file_name: P, append: bool) -> File {
        let file_path = self.file_path(file_name.as_ref());
        if let Some(dir) = file_path.parent() {
            tokio::fs::create_dir_all(dir).await.unwrap();
        }
        self.files.insert(file_name.as_ref().to_path_buf());
        if append {
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(file_path)
                .await
                .unwrap()
        } else {
            File::create(file_path).await.unwrap()
        }
    }

    pub async fn delete<P: AsRef<Path>>(&mut self, file_name: P) -> bool {
        if !self.files.contains(file_name.as_ref()) {
            return false;
        }
        self.files.remove(file_name.as_ref());
        tokio::fs::remove_file(self.file_path(file_name.as_ref()))
            .await
            .unwrap();
        true
    }

    pub async fn get_file<P: AsRef<Path>>(&self, file_name: P) -> std::io::Result<File> {
        File::open(self.file_path(file_name.as_ref())).await
    }

    pub fn clear(&mut self) {
        self.files.clear();
    }
}
