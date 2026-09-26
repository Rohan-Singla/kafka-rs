use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub struct OffsetStore {
    path: PathBuf,
    journal: Mutex<File>,
    offsets: DashMap<OffsetKey, u64>,
    fsync: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OffsetKey {
    pub group: String,
    pub topic: String,
    pub partition: u32,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    g: String,
    t: String,
    p: u32,
    o: u64,
}

impl OffsetStore {
    pub fn open(path: PathBuf, fsync: bool) -> Result<Self> {
        let offsets = Self::replay(&path)?;

        Self::compact(&path, &offsets)?;

        let journal = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            journal: Mutex::new(journal),
            offsets,
            fsync,
        })
    }

    fn replay(path: &Path) -> Result<DashMap<OffsetKey, u64>> {
        let offsets = DashMap::new();
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(offsets),
            Err(e) => return Err(e.into()),
        };

        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Entry>(&line) {
                Ok(entry) => {
                    offsets.insert(
                        OffsetKey {
                            group: entry.g,
                            topic: entry.t,
                            partition: entry.p,
                        },
                        entry.o,
                    );
                }
                Err(e) => tracing::warn!("skipping unreadable offset journal line: {}", e),
            }
        }
        Ok(offsets)
    }

    fn compact(path: &Path, offsets: &DashMap<OffsetKey, u64>) -> Result<()> {
        if offsets.is_empty() && !path.exists() {
            return Ok(());
        }

        let temp_path = path.with_extension("compacting");
        let mut temp = File::create(&temp_path)?;
        for entry in offsets.iter() {
            let key = entry.key();
            let line = serde_json::to_string(&Entry {
                g: key.group.clone(),
                t: key.topic.clone(),
                p: key.partition,
                o: *entry.value(),
            })?;
            temp.write_all(line.as_bytes())?;
            temp.write_all(b"\n")?;
        }
        temp.sync_all()?;
        drop(temp);

        std::fs::rename(&temp_path, path)?;
        Ok(())
    }

    pub fn commit(&self, group: &str, topic: &str, partition: u32, offset: u64) -> Result<()> {
        let line = serde_json::to_string(&Entry {
            g: group.to_string(),
            t: topic.to_string(),
            p: partition,
            o: offset,
        })?;

        {
            let mut journal = self.journal.lock().unwrap_or_else(|e| e.into_inner());
            journal.write_all(line.as_bytes())?;
            journal.write_all(b"\n")?;
            if self.fsync {
                journal.sync_data()?;
            }
        }

        self.offsets.insert(
            OffsetKey {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
            },
            offset,
        );
        Ok(())
    }

    pub fn fetch(&self, group: &str, topic: &str, partition: u32) -> u64 {
        self.offsets
            .get(&OffsetKey {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
            })
            .map(|v| *v)
            .unwrap_or(0)
    }

    pub fn groups(&self) -> Vec<String> {
        let mut names: Vec<String> = self.offsets.iter().map(|e| e.key().group.clone()).collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    pub fn committed_for_group(&self, group: &str) -> Vec<(String, u32, u64)> {
        let mut rows: Vec<(String, u32, u64)> = self
            .offsets
            .iter()
            .filter(|e| e.key().group == group)
            .map(|e| (e.key().topic.clone(), e.key().partition, *e.value()))
            .collect();
        rows.sort();
        rows
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("__offsets.json");

        {
            let store = OffsetStore::open(path.clone(), false).unwrap();
            store.commit("group-a", "orders", 0, 42).unwrap();
            store.commit("group-a", "orders", 1, 7).unwrap();
            store.commit("group-b", "orders", 0, 3).unwrap();
        }

        let store = OffsetStore::open(path, false).unwrap();
        assert_eq!(store.fetch("group-a", "orders", 0), 42);
        assert_eq!(store.fetch("group-a", "orders", 1), 7);
        assert_eq!(store.fetch("group-b", "orders", 0), 3);
        assert_eq!(store.fetch("group-c", "orders", 0), 0);
    }

    #[test]
    fn last_commit_wins() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("__offsets.json");

        {
            let store = OffsetStore::open(path.clone(), false).unwrap();
            for offset in [5u64, 10, 25] {
                store.commit("g", "t", 0, offset).unwrap();
            }
        }

        let store = OffsetStore::open(path, false).unwrap();
        assert_eq!(store.fetch("g", "t", 0), 25);
    }

    #[test]
    fn compaction_shrinks_the_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("__offsets.json");

        {
            let store = OffsetStore::open(path.clone(), false).unwrap();
            for offset in 0..500u64 {
                store.commit("g", "t", 0, offset).unwrap();
            }
        }
        let before = std::fs::metadata(&path).unwrap().len();

        let store = OffsetStore::open(path.clone(), false).unwrap();
        let after = std::fs::metadata(&path).unwrap().len();

        assert_eq!(store.fetch("g", "t", 0), 499);
        assert!(
            after < before,
            "expected compaction, {} -> {}",
            before,
            after
        );
    }

    #[test]
    fn a_torn_final_line_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("__offsets.json");

        {
            let store = OffsetStore::open(path.clone(), false).unwrap();
            store.commit("g", "t", 0, 11).unwrap();
        }
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"g\":\"g\",\"t\":\"t\",\"p\":0,\"o\":9")
            .unwrap();
        drop(file);

        let store = OffsetStore::open(path, false).unwrap();
        assert_eq!(store.fetch("g", "t", 0), 11);
    }
}
