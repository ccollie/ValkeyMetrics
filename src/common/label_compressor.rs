use dashmap::DashMap;
use metricsql_runtime::Label;
use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/lib/promutils/labelscompressor.go#L15
static LABELS_COMPRESSOR_POOL: LazyLock<Mutex<Vec<LabelsCompressor>>> = LazyLock::new(|| Mutex::new(Vec::new()));

pub struct LabelsCompressor {
    label_to_idx: DashMap<Label, u64>,
    idx_to_label: LabelsMap,
    next_idx: AtomicU64,
    total_size_bytes: AtomicU64,
}

impl LabelsCompressor {
    pub fn size_bytes(&self) -> u64 {
        unsafe {
            size_of_val(self) as u64 + self.total_size_bytes.load(Ordering::Relaxed)
        }
    }

    pub fn items_count(&self) -> u64 {
        self.next_idx.load(Ordering::Relaxed)
    }

    pub fn compress(&self, dst: Vec<u8>, labels: Vec<Label>) -> Vec<u8> {
        if labels.is_empty() {
            return vec![0];
        }

        let mut a = vec![0; labels.len() + 1];
        a[0] = labels.len() as u64;
        self.compress_inner(&mut a[1..], labels);
        encoding::marshal_var_uint64s(&mut dst, &a);
        dst
    }

    fn compress_inner(&self, dst: &mut [u64], labels: Vec<Label>) {
        for (i, label) in labels.iter().enumerate() {
            let v = self.label_to_idx.entry(label.clone()).or_insert_with(|| {
                let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
                let label_copy = clone_label(label);
                self.idx_to_label.store(idx, label_copy.clone());
                idx
            });
            dst[i] = *v;
        }
    }

    pub fn decompress(&self, dst: Vec<Label>, src: Vec<u8>) -> Vec<Label> {
        let (labels_len, n_size) = encoding::unmarshal_var_uint64(&src);
        let tail = &src[n_size..];
        if labels_len == 0 {
            return dst;
        }

        let mut a = vec![0; labels_len as usize];
        let (_, err) = encoding::unmarshal_var_uint64s(&mut a, tail);
        if err.is_some() {
            panic!("BUG: cannot unmarshal label indexes: {:?}", err);
        }
        self.decompress_inner(dst, a)
    }

    fn decompress_inner(&self, mut dst: Vec<Label>, src: Vec<u64>) -> Vec<Label> {
        for idx in src {
            let label = self.idx_to_label.load(idx).expect("BUG: missing label for idx");
            dst.push(label);
        }
        dst
    }
}

fn clone_label(label: &Label) -> Label {
    let mut buf = Vec::with_capacity(label.name.len() + label.value.len());
    buf.extend_from_slice(label.name.as_bytes());
    buf.extend_from_slice(label.value.as_bytes());
    let label_name = String::from_utf8_lossy(&buf[..label.name.len()]).to_string();
    let label_value = String::from_utf8_lossy(&buf[label.name.len()..]).to_string();
    Label {
        name: label_name,
        value: label_value,
    }
}

struct LabelsMap {
    read_only: AtomicPtr<Vec<Option<Label>>>,
    mutable: Mutex<HashMap<u64, Label>>,
    misses: AtomicU64,
}

impl LabelsMap {
    fn store(&self, idx: u64, label: Label) {
        let mut mutable = self.mutable.lock().unwrap();
        mutable.insert(idx, label);
    }

    fn load(&self, idx: u64) -> Option<Label> {
        if let Some(read_only) = unsafe { self.read_only.load().as_ref() } {
            if idx < read_only.len() as u64 {
                if let Some(label) = &read_only[idx as usize] {
                    return Some(label.clone());
                }
            }
        }
        self.load_slow(idx)
    }

    fn load_slow(&self, idx: u64) -> Option<Label> {
        let mut mutable = self.mutable.lock().unwrap();
        if let Some(read_only) = unsafe { self.read_only.load().as_ref() } {
            if idx < read_only.len() as u64 {
                if let Some(label) = &read_only[idx as usize] {
                    return Some(label.clone());
                }
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let label = mutable.get(&idx).cloned();
        if label.is_none() {
            self.move_mutable_to_read_only_locked();
            self.misses.store(0, Ordering::Relaxed);
        }
        label
    }

    fn move_mutable_to_read_only_locked(&self) {
        let mut mutable = self.mutable.lock().unwrap();
        if mutable.is_empty() {
            return;
        }

        let mut labels = if let Some(read_only) = unsafe { self.read_only.load().as_ref() } {
            read_only.clone()
        } else {
            Vec::new()
        };

        for (idx, label) in mutable.iter() {
            if *idx < labels.len() as u64 {
                labels[*idx as usize] = Some(label.clone());
            } else {
                while *idx > labels.len() as u64 {
                    labels.push(None);
                }
                labels.push(Some(label.clone()));
            }
        }
        mutable.clear();
        self.read_only.store(Box::into_raw(Box::new(labels)));
    }
}

mod encoding {
    pub fn marshal_var_uint64s(dst: &mut Vec<u8>, src: &[u64]) {
        for &v in src {
            let mut buf = [0u8; 10];
            let mut n = 0;
            let mut v = v;
            while v >= 0x80 {
                buf[n] = (v as u8) | 0x80;
                v >>= 7;
                n += 1;
            }
            buf[n] = v as u8;
            n += 1;
            dst.extend_from_slice(&buf[..n]);
        }
    }

    pub fn unmarshal_var_uint64(src: &[u8]) -> (u64, usize) {
        let mut result = 0;
        let mut shift = 0;
        let mut i = 0;
        while i < src.len() {
            let b = src[i];
            result |= (b as u64 & 0x7F) << shift;
            if b < 0x80 {
                return (result, i + 1);
            }
            shift += 7;
            i += 1;
        }
        (0, 0)
    }

    pub fn unmarshal_var_uint64s(dst: &mut [u64], src: &[u8]) -> (usize, Option<&str>) {
        let mut i = 0;
        for d in dst.iter_mut() {
            let (v, n) = unmarshal_var_uint64(&src[i..]);
            if n == 0 {
                return (i, Some("unexpected end of src"));
            }
            *d = v;
            i += n;
        }
        (i, None)
    }
}

#[cfg(test)]
mod tests {
    use metricsql_runtime::Label;
    use crate::common::label_compressor::LabelsCompressor;

    fn test_labels_compressor_serial() {
        let mut lc: LabelsCompressor = LabelsCompressor::new();

        fn f(labels: &[Label]) {
            let s_expected = labels_to_string(labels);
            let data = lc.compress(nil, labels);
            let labels_result = lc.decompress(nil, data);
            let sResult = labels_to_string(labels_result);
            if s_expected != sResult {
                t.Fatalf("unexpected result; got %s; want %s", sResult, s_expected)
            }

            if labels.len() > 0 {
                if lc.SizeBytes() == 0 {
                    t.Fatalf("Unexpected zero SizeBytes()")
                }
                if lc.ItemsCount() == 0 {
                    t.Fatalf("Unexpected zero ItemsCount()")
                }
            }
        }

    // empty labels
    f(&[]);

    // non-empty labels
    f(&[Label{
                name: "instance".into(),
                value: "12345.4342.342.3".into(),
                },
                Label{
                    name:  "job".into(),
                    value: "kube-pod-12323".into(),
                }
            ]);

        f(&[Label{
                name:  "instance".into(),
                value: "12345.4342.342.3".into(),
            },
            Label{
                name:  "job".into(),
                value: "kube-pod-123124".into(),
            },
            Label{
                name:  "pod".into(),
                value: "foo-bar-baz".into(),
            }]);
    }

    fn test_labels_compressor_concurrent() {
        let concurrency = 5;
        let mut lc: LabelsCompressor = LabelsCompressor::new();

        for i in 0 .. concurrency {
            go func() {
                let series = new_test_series(100, 20);
                for (i, labels) in series.iter().enumerate() {
                    let sExpected = labels_to_string(labels);
                    let data = lc.compress(nil, labels);
                    let labels_result = lc.Decompress(nil, data);
                    let sResult = labels_to_string(labels_result);
                    if sExpected != sResult {
                        panic(fmt.Errorf("unexpected result on iteration %d; got %s; want %s", i, sResult, sExpected))
                    }
                }
            }()
        }

        if lc.size_bytes() == 0 {
            t.Fatalf("Unexpected zero SizeBytes()")
        }
        if lc.items_count() == 0 {
            t.Fatalf("Unexpected zero ItemsCount()")
        }
    }

    fn labels_to_string(labels: &[Label]) -> String {
        let l = Labels{
            Labels: labels,
        };
        l.to_string()
    }

    fn new_test_series(series_count: usize, labels_per_series: usize) -> Vec<Vec<Label>> {
        let mut series: Vec<Vec<Label>> = Vec::with_capacity(series_count);
        for i in 0..series_count {
            let mut labels: Vec<Label> = Vec::with_capacity(labels_per_series);
            for j in 0 ..labels_per_series {
                labels.push(Label{
                    name:  format!("label_{j}"),
                    value: format!("value_{i}_{j}"),
                })
            }
            series.push(labels);
        }
        series
    }
}