//! Hashing and encoding functions for turbopack.
//!
//! An example use of this module is hashing a file's content for cache
//! invalidation, and encoding the hash to an hexadecimal string for use in a
//! file name.

use std::collections::HashMap;

mod deterministic_hash;
mod hex;
mod xxh3_hash64;

pub use crate::{
    deterministic_hash::{DeterministicHash, DeterministicHasher},
    hex::encode_hex,
    xxh3_hash64::{Xxh3Hash64Hasher, hash_xxh3_hash64},
};

/// Characters valid as the first character of a JavaScript identifier.
/// Uses base-53: a-z, A-Z, _ (excludes $ to avoid conflicts with generated code)
const BASE53_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";

/// Characters valid as subsequent characters of a JavaScript identifier.
/// Uses base-63: a-z, A-Z, 0-9, _ (excludes $ to avoid conflicts)
const BASE63_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

/// Encode a hash value to a JavaScript-safe identifier of the given length.
fn encode_to_identifier(hash: u64, length: usize) -> String {
    if length == 0 {
        return String::new();
    }

    let mut result = Vec::with_capacity(length);
    let mut value = hash;

    // First character uses base-53
    result.push(BASE53_CHARS[(value % 53) as usize]);
    value /= 53;

    // Subsequent characters use base-63
    for _ in 1..length {
        result.push(BASE63_CHARS[(value % 63) as usize]);
        value /= 63;
    }

    // Safety: all characters are ASCII
    unsafe { String::from_utf8_unchecked(result) }
}

/// Shorten a list of (key, value_to_hash) pairs to unique short identifiers.
///
/// Takes items and generates short, unique JavaScript-safe identifiers for each.
/// The resulting identifiers are deterministic (same input produces same output)
/// and as short as possible while remaining unique within the set.
///
/// # Arguments
/// * `items` - List of (key, value_to_hash) pairs. The key is returned as-is, and value_to_hash is
///   hashed to generate the short identifier.
/// * `min_length` - Minimum length for generated identifiers (must be >= 1)
///
/// # Returns
/// Vector of (original_key, short_identifier) pairs
pub fn shorten_to_unique_hashes<K>(items: Vec<(K, String)>, min_length: usize) -> Vec<(K, String)> {
    if items.is_empty() {
        return Vec::new();
    }

    let min_length = min_length.max(1);

    // Hash all items
    let hashed: Vec<(K, u64)> = items
        .into_iter()
        .map(|(key, value)| {
            let hash = hash_xxh3_hash64(value.as_str());
            (key, hash)
        })
        .collect();

    // Try increasing lengths until all identifiers are unique
    let mut length = min_length;
    loop {
        let mut seen: HashMap<String, usize> = HashMap::with_capacity(hashed.len());
        let mut has_collision = false;

        for (idx, (_, hash)) in hashed.iter().enumerate() {
            let encoded = encode_to_identifier(*hash, length);
            if let Some(&existing_idx) = seen.get(&encoded) {
                // Collision detected - only a real collision if hashes differ
                if hashed[existing_idx].1 != *hash {
                    has_collision = true;
                    break;
                }
            }
            seen.insert(encoded, idx);
        }

        if !has_collision {
            // All unique at this length
            return hashed
                .into_iter()
                .map(|(key, hash)| {
                    let encoded = encode_to_identifier(hash, length);
                    (key, encoded)
                })
                .collect();
        }

        length += 1;

        // Safety limit - 11 chars of base63 can represent 2^64 values
        if length > 11 {
            // Fall back to full hash encoding
            return hashed
                .into_iter()
                .map(|(key, hash)| {
                    let encoded = encode_to_identifier(hash, 11);
                    (key, encoded)
                })
                .collect();
        }
    }
}
