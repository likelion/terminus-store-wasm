use std::{io, path::Path};

use crate::storage::{BaseLayerFiles, FileLoad, FileStore};

#[allow(dead_code)]
fn map_triple(
    triple: (u64, u64, u64),
    node_map: &[usize],
    predicate_map: &[usize],
    value_map: &[usize],
    num_nodes: usize,
) -> (u64, u64, u64) {
    let s = node_map[triple.0 as usize - 1] as u64 + 1;
    let p = predicate_map[triple.1 as usize - 1] as u64 + 1;

    let o = if (triple.2 as usize - 1) < node_map.len() {
        node_map[triple.2 as usize - 1] as u64 + 1
    } else {
        value_map[triple.2 as usize - 1 - node_map.len()] as u64 + num_nodes as u64 + 1
    };

    (s, p, o)
}

pub fn merge_base_layers<F: FileLoad + FileStore + 'static, P: AsRef<Path>>(
    _inputs: &[BaseLayerFiles<F>],
    _output: BaseLayerFiles<F>,
    _temp_path: P,
) -> io::Result<()> {
    // TODO: This function needs to be reimplemented without stream-based APIs.
    // The original implementation used tdb-succinct stream APIs (TfcDictStream,
    // dedup_merge_string_dictionaries_stream, heap_sorted_stream) which were
    // removed during async stripping. A synchronous implementation using
    // iterators is needed.
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "merge_base_layers not yet implemented for synchronous API",
    ))
}
