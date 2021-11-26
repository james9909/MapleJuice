use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// From: https://users.rust-lang.org/t/how-to-split-a-slice-into-n-chunks/40008/5
pub fn range<'a, T>(collection: &'a [T], n: usize) -> Vec<&'a [T]>
where
    T: Clone,
{
    let len = collection.len();
    if n > len {
        collection.chunks(1).collect()
    } else {
        assert!(len >= n);
        let (quo, rem) = (len / n, len % n);
        let split = (quo + 1) * rem;
        collection[..split]
            .chunks(quo + 1)
            .chain(collection[split..].chunks(quo))
            .collect()
    }
}

pub fn hash<'a, T: Hash>(collection: &'a [T], n: usize) -> Vec<Vec<&'a T>> {
    let mut res = Vec::with_capacity(n);
    for _ in 0..n {
        res.push(Vec::new());
    }
    for item in collection {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let idx = (hasher.finish() % (n as u64)) as usize;
        res[idx].push(item);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let collection = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(
            range(&collection, 3),
            vec![vec![1, 2, 3, 4], vec![5, 6, 7], vec![8, 9, 10]]
        );
        assert_eq!(
            range(&collection, 1),
            vec![vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]
        );
        assert_eq!(
            range(&collection, 10),
            vec![
                vec![1],
                vec![2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
                vec![8],
                vec![9],
                vec![10],
            ]
        );
    }

    #[test]
    fn test_partition() {
        let collection = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let partitioned = hash(&collection, 3);
        assert_eq!(partitioned.len(), 3);

        let mut flattened: Vec<_> = partitioned.into_iter().flatten().cloned().collect();
        flattened.sort();
        assert_eq!(flattened, collection);
    }
}
