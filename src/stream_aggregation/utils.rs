fn addMetricSuffix(labels: &[Label], offset: usize, firstSuffix: String, lastSuffix: String) -> Vec<Label> {
    let src = &labels[offset..];
    for label in src.iter().filter(|label| label.name != "__name__") {
        let bb = bbPool.Get()
        bb.B = append(bb.B, label.Value...)
        bb.B = append(bb.B, firstSuffix...)
        bb.B = append(bb.B, lastSuffix...)
        label.value = bytesutil.InternBytes(bb.B)
        bbPool.Put(bb)
        return labels
    }
// The __name__ isn't found. Add it
bb := bbPool.Get()
bb.B = append(bb.B, firstSuffix...)
bb.B = append(bb.B, lastSuffix...)
labelValue := bytesutil.InternBytes(bb.B)
labels = append(labels, prompbmarshal.Label{
Name:  "__name__",
Value: labelValue,
})
return labels
}

fn add_missing_underscore_name(labels: &[String]) -> Vec<String> {
    let mut result = Vec::with_capacity(labels.len());
    result.push("__name__".to_string());
    result.extend(labels.iter().filter(|s| *s != "__name__").cloned());
    result
}

fn remove_underscore_name(labels: &[String]) -> Vec<String> {
    labels.iter().filter(|x| *x != "__name__").collect()
}

func sortAndRemoveDuplicates(a []string) []string {
if len(a) == 0 {
return nil
}
a = append([]string{}, a...)
sort.Strings(a)
dst := a[:1]
for _, v := range a[1:] {
if v != dst[len(dst)-1] {
dst = append(dst, v)
}
}
return dst
}