use rand_distr::num_traits::Zero;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

#[derive(Clone, Hash)]
pub struct Label {
    name: String,
    value: String,
}

pub struct LabelsCompressor {
    label_to_idx: dashmap::DashMap<Label, u64>,
    idx_to_label: RwLock<LabelsMap>,
    next_idx: AtomicU64,
    total_size_bytes: AtomicU64,
}

impl LabelsCompressor {
    pub fn new() -> Self {
        Self {
            label_to_idx: dashmap::DashMap::new(),
            idx_to_label: RwLock::new(LabelsMap::new()),
            next_idx: AtomicU64::new(0),
            total_size_bytes: AtomicU64::new(0),
        }
    }

    pub fn size_bytes(&self) -> u64 {
        size_of::<Self>() as u64 + self.total_size_bytes.load(Ordering::Relaxed)
    }

    pub fn items_count(&self) -> u64 {
        self.next_idx.load(Ordering::Relaxed)
    }

    pub fn compress(&self, dst: &mut Vec<u8>, labels: &[Label]) {
        if labels.is_empty() {
            dst.push(0);
            return;
        }

        let mut a = Vec::with_capacity(labels.len() + 1);
        a.push(labels.len() as u64);
        self.compress_inner(&mut a, labels);
        encoding::marshal_var_uint64s(dst, &a);
    }

    fn compress_inner(&self, dst: &mut Vec<u64>, labels: &[Label]) {
        let mut idx_to_label = self.idx_to_label.write().unwrap();
        for label in labels {
            let idx = self.label_to_idx.entry(label).or_insert_with(|| {
                let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
                idx_to_label.store(idx, label.clone());
                let label_size_bytes = label.name.len() as u64 + label.value.len() as u64;
                let entry_size_bytes = label_size_bytes + size_of::<Label>() as u64 + size_of::<u64>() as u64;
                self.total_size_bytes.fetch_add(entry_size_bytes, Ordering::Relaxed);
                idx
            });
            dst.push(*idx);
        }
    }

    pub fn decompress(&self, dst: &mut Vec<Label>, src: &[u8]) -> Result<(), String>{
        let (labels_len, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size == 0 {
            panic!("BUG: cannot unmarshal labels length from uvarint");
        }
        let tail = &src[n_size..];
        if labels_len == 0 {
            if !tail.is_empty() {
                return Err(format!("BUG: unexpected non-empty tail left; len(tail)={}", tail.len()));
            }
            return Ok(());
        }

        let mut a = Vec::with_capacity(labels_len as usize);
        let (unmarshaled, err) = encoding::unmarshal_var_uint64s(&mut a, tail);
        if err.is_some() {
            return Err(format!("BUG: cannot unmarshal label indexes: {}", err.unwrap()));
        }
        if !unmarshaled.is_zero() {
            return Err(format!("BUG: unexpected non-empty tail left: len(tail)={}", unmarshaled.len()));
        }
        self.decompress_inner(dst, &a);
        Ok(())
    }

    fn decompress_inner(&self, dst: &mut Vec<Label>, src: &[u64]) {
        let mut idx_to_label = self.idx_to_label.write().unwrap();
        for idx in src {
            if let Some(label) = idx_to_label.load(*idx) {
                dst.push(label);
            } else {
                panic!("BUG: missing label for idx={}", idx);
            }
        }
    }
}

pub struct LabelsMap {
    read_only: Vec<Option<Label>>,
    mutable: HashMap<u64, Label>,
    misses: u64,
}

impl LabelsMap {
    pub fn new() -> Self {
        Self {
            read_only: Vec::new(),
            mutable: HashMap::new(),
            misses: 0,
        }
    }

    pub fn store(&mut self, idx: u64, label: Label) {
        self.mutable.insert(idx, label);
    }

    pub fn load(&mut self, idx: u64) -> Option<Label> {
        if let Some(label) = self.read_only.get(idx as usize) {
            return label.clone();
        }
        self.load_slow(idx)
    }

    fn load_slow(&mut self, idx: u64) -> Option<Label> {
        if let Some(label) = self.read_only.get(idx as usize) {
            return label.clone();
        }
        self.misses += 1;
        if let Some(label) = self.mutable.get(&idx) {
            if self.misses > self.read_only.len() as u64 {
                self.move_mutable_to_read_only_locked(read_only);
                self.misses = 0;
            }
            return Some(label.clone());
        }
        None
    }

    fn move_mutable_to_read_only_locked(&mut self, read_only: &Vec<Option<Label>>) {
        if self.mutable.is_empty() {
            return;
        }

        let mut labels = read_only.clone();
        for (idx, label) in self.mutable.iter() {
            if *idx < labels.len() as u64 {
                labels[*idx as usize] = Some(label.clone());
            } else {
                while *idx > labels.len() as u64 {
                    labels.push(None);
                }
                labels.push(Some(label.clone()));
            }
        }
        self.mutable.clear();
        self.read_only.extend(labels);
    }
}

mod encoding {
    pub fn marshal_var_uint64s(dst: &mut Vec<u8>, src: &[u64]) {
        for &v in src {
            marshal_var_uint64(dst, v);
        }
    }

    pub fn marshal_var_uint64(dst: &mut Vec<u8>, v: u64) {
        let mut v = v;
        while v >= 0x80 {
            dst.push((v as u8) | 0x80);
            v >>= 7;
        }
        dst.push(v as u8);
    }

    pub fn unmarshal_var_uint64s(dst: &mut Vec<u64>, src: &[u8]) -> (usize, Option<&'static str>) {
        let mut i = 0;
        while i < src.len() {
            let (v, n) = unmarshal_var_uint64(&src[i..]);
            if n == 0 {
                return (i, Some("unexpected end of buffer"));
            }
            dst.push(v);
            i += n;
        }
        (i, None)
    }

    pub fn unmarshal_var_uint64(src: &[u8]) -> (u64, usize) {
        let mut v = 0;
        let mut shift = 0;
        let mut i = 0;
        while i < src.len() {
            let b = src[i];
            v |= (b as u64 & 0x7F) << shift;
            if b < 0x80 {
                return (v, i + 1);
            }
            shift += 7;
            i += 1;
        }
        (0, 0)
    }
}