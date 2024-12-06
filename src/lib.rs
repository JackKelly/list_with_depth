use object_store::{path::Path, ListResult, ObjectStore};

pub trait ListWithDepth: ObjectStore {
    /// List objects with the given prefix and depth, and an implementation specific delimiter.
    /// Returns common prefixes (directories) in addition to object metadata.
    ///
    /// For example, say that a bucket contains the following objects:
    /// - `a.txt`
    /// - `foo/b.txt`
    /// - `foo/bar/c.txt`
    /// - `foo/bar/d.txt`
    ///
    /// Calling `list_with_depth` with `prefix = None` and `depth = 0` is equivalent to calling
    /// `ObjectStore::list_with_delimiter(None)`. It will return the objects and common prefixes at
    /// the root: `a.txt` and `foo/`.
    ///
    /// Calling `list_with_depth` with `depth = 1` will recurse once, and return `b.txt` and
    /// `foo/bar/`.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar` is a prefix of `foo/bar/x`
    /// but not of `foo/bar_baz/x`.
    async fn list_with_depth(
        &self,
        prefix: Option<&Path>,
        depth: usize,
    ) -> object_store::Result<ListResult> {
        Ok(ListResult {
            objects: vec![],
            common_prefixes: vec![],
        })
    }
}

impl<T: ObjectStore> ListWithDepth for T {}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, PutPayload};

    use super::*;

    async fn create_in_memory_store() -> object_store::Result<InMemory> {
        const KEYS: [&str; 4] = ["a.txt", "foo/b.txt", "foo/bar/c.txt", "foo/bar/d.txt"];
        let store = InMemory::new();
        for key in KEYS {
            store.put(&key.into(), PutPayload::new()).await?;
        }
        Ok(store)
    }

    #[tokio::test]
    async fn test_list_with_depth_0() -> object_store::Result<()> {
        let store = create_in_memory_store().await?;
        let list_result = store.list_with_depth(None, 0).await?;
        let ListResult {
            objects,
            common_prefixes,
        } = list_result;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location, Path::from("a.txt"));
        assert_eq!(common_prefixes, vec![Path::from("foo")]);
        Ok(())
    }
}
