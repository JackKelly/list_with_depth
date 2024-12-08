use std::{future::Future, pin::Pin, sync::Arc};

use object_store::{path::Path, ListResult, ObjectStore};
use tokio::task::JoinSet;

/// List objects with the given prefix and depth, and an implementation specific delimiter.
/// Returns common prefixes (directories) in addition to object metadata.
///
/// For example, say that a bucket contains the following objects:
/// - `a.txt`
/// - `foo/b.txt`
/// - `foo/bar/c.txt`
/// - `foo/bar/d.txt`
///
/// Calling `list_with_depth` with `depth = 0` is equivalent to calling
/// `ObjectStore::list_with_delimiter`: It will return the objects and common
/// prefixes at the root: `objects="a.txt"` and `common_prefixes="foo"`.
///
/// Calling `list_with_depth` with `depth = 1` will recurse once, and return
/// `objects="foo/b.txt"` and `common_prefixes="foo/bar"`.
///
/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar` is a
/// prefix of `foo/bar/x` but not of `foo/bar_baz/x`.
pub async fn list_with_depth(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
    depth: usize,
) -> object_store::Result<ListResult> {
    let list_result = store.list_with_delimiter(prefix).await?;
    next_level(store, list_result, 0, depth).await
}

fn next_level(
    store: Arc<dyn ObjectStore>,
    list_result: ListResult,
    depth_of_list_result: usize,
    target_depth: usize,
) -> Pin<Box<dyn Future<Output = std::result::Result<ListResult, object_store::Error>> + Send>> {
    // See here for why we're using `Box::pin`:
    // https://stackoverflow.com/a/67030773
    Box::pin(async move {
        // Base case:
        if depth_of_list_result == target_depth {
            return Ok(list_result);
        }

        let mut set = JoinSet::new();
        for common_prefix in list_result.common_prefixes {
            let inner_store = store.clone();
            set.spawn(async move {
                let next_list_result = inner_store
                    .list_with_delimiter(Some(&common_prefix))
                    .await?;

                // Recursive call to next_level:
                next_level(
                    inner_store,
                    next_list_result,
                    depth_of_list_result + 1,
                    target_depth,
                )
                .await
            });
        }

        // Extract results and propagate errors:
        let mut combined = ListResult {
            objects: vec![],
            common_prefixes: vec![],
        };
        while let Some(handle) = set.join_next().await {
            let list_res = handle??;
            combined.objects.extend(list_res.objects);
            combined.common_prefixes.extend(list_res.common_prefixes);
        }
        Ok(combined)
    })
}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, PutPayload};

    use super::*;

    async fn create_in_memory_store() -> object_store::Result<InMemory> {
        const KEYS: [&str; 6] = [
            "a.txt",
            "foo/b.txt",
            "foo/bar/c.txt",
            "foo/bar/d.txt",
            "foo/baz/e.txt",
            "foo/baz/bleh/f.txt",
        ];
        let store = InMemory::new();
        for key in KEYS {
            store.put(&key.into(), PutPayload::new()).await?;
        }
        Ok(store)
    }

    /// Returns (object_paths, common_prefixes).
    async fn test_depth_n(depth: usize) -> object_store::Result<(Vec<Path>, Vec<Path>)> {
        let store = create_in_memory_store().await?;
        let store = Arc::new(store);
        let ListResult {
            objects,
            common_prefixes,
        } = list_with_depth(store, None, depth).await?;
        let object_paths = objects
            .into_iter()
            .map(|object_meta| object_meta.location)
            .collect();
        Ok((object_paths, common_prefixes))
    }

    #[tokio::test]
    async fn test_depth_0() -> object_store::Result<()> {
        let (object_paths, common_prefixes) = test_depth_n(0).await?;
        assert_eq!(object_paths.len(), 1);
        assert_eq!(object_paths[0], Path::from("a.txt"));
        assert_eq!(common_prefixes, vec![Path::from("foo")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_depth_1() -> object_store::Result<()> {
        let (object_paths, common_prefixes) = test_depth_n(1).await?;
        assert_eq!(object_paths.len(), 1);
        assert_eq!(object_paths[0], Path::from("foo/b.txt"));
        assert_eq!(
            common_prefixes,
            vec![Path::from("foo/bar"), Path::from("foo/baz")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_depth_2() -> object_store::Result<()> {
        let (object_paths, common_prefixes) = test_depth_n(2).await?;
        assert_eq!(object_paths.len(), 3);
        assert_eq!(object_paths[0], Path::from("foo/bar/c.txt"));
        assert_eq!(object_paths[1], Path::from("foo/bar/d.txt"));
        assert_eq!(object_paths[2], Path::from("foo/baz/e.txt"));
        assert_eq!(common_prefixes, vec![Path::from("foo/baz/bleh")]);
        Ok(())
    }
}
