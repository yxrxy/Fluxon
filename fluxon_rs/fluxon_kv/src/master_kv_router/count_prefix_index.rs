use std::collections::HashMap;

/// Radix tree node used for prefix counting.
///
/// Each node tracks the total number of keys in its subtree via `count`.
#[derive(Default)]
struct Node {
    count: u64,
    children: HashMap<u8, Node>,
}

impl Node {
    fn new() -> Self {
        Self {
            count: 0,
            children: HashMap::new(),
        }
    }
}

/// Simple radix tree for string prefixes, storing counts only.
///
/// Callers must ensure `insert` and `remove` are balanced for each key;
/// violations are treated as logic bugs and will panic.
#[derive(Default)]
pub struct PrefixRadixTree {
    root: Node,
}

impl PrefixRadixTree {
    pub fn new() -> Self {
        Self { root: Node::new() }
    }

    /// Insert a new key. Must only be called once per logical key.
    pub fn insert(&mut self, key: &str) {
        let bytes = key.as_bytes();
        let mut node = &mut self.root;
        node.count = node
            .count
            .checked_add(1)
            .expect("PrefixRadixTree count overflow on insert (root)");

        for &b in bytes {
            node = node.children.entry(b).or_insert_with(Node::new);
            node.count = node
                .count
                .checked_add(1)
                .expect("PrefixRadixTree count overflow on insert (child)");
        }
    }

    /// Remove an existing key. Must only be called for keys that were inserted.
    pub fn remove(&mut self, key: &str) {
        fn remove_inner(node: &mut Node, bytes: &[u8], idx: usize) -> bool {
            node.count = node
                .count
                .checked_sub(1)
                .expect("PrefixRadixTree underflow on remove");

            if idx == bytes.len() {
                return node.count == 0;
            }

            let b = bytes[idx];
            let child = node
                .children
                .get_mut(&b)
                .expect("PrefixRadixTree remove: missing child for existing key");
            let should_prune = remove_inner(child, bytes, idx + 1);
            if should_prune {
                node.children.remove(&b);
            }
            node.count == 0
        }

        remove_inner(&mut self.root, key.as_bytes(), 0);
    }

    /// Count keys whose name starts with the given prefix.
    pub fn count_prefix(&self, prefix: &str) -> u64 {
        let mut node = &self.root;
        for &b in prefix.as_bytes() {
            match node.children.get(&b) {
                Some(child) => node = child,
                None => return 0,
            }
        }
        node.count
    }
}
