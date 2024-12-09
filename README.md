List objects with the given prefix and depth, and an implementation specific delimiter.
Returns common prefixes (directories) in addition to object metadata.

For example, say that a bucket contains the following objects:
- `a.txt`
- `foo/b.txt`
- `foo/bar/c.txt`
- `foo/bar/d.txt`

Calling [`list_with_depth`] with `depth = 0` is equivalent to calling
[`ObjectStore::list_with_delimiter`]: It will return the objects and common
prefixes at the root: `objects=["a.txt"]` and `common_prefixes=["foo"]`.

Calling [`list_with_depth`] with `depth = 1` will recurse once, and return
`objects=["foo/b.txt"]` and `common_prefixes=["foo/bar"]`.

The equivalent commands at a Unix command line would be:
- `ls *` (depth=0)
- `ls */*` (depth=1)
- `ls */*/*` (depth=2)
- etc.

Prefixes are evaluated on a path segment basis, i.e. `foo/bar` is a
prefix of `foo/bar/x` but not of `foo/bar_baz/x`.

## Example

```rust
use std::sync::Arc;
use list_with_depth::list_with_depth;
use object_store::{memory::InMemory, PutPayload, ObjectStore, ListResult, path::Path};

#[tokio::main(flavor = "current_thread")]
async fn main() -> object_store::Result<()> {

    // Create some objects in memory:
    const KEYS: [&str; 4] = [
        "a.txt",
        "foo/b.txt",
        "foo/bar/c.txt",
        "foo/bar/d.txt",
    ];
    let store = Arc::new(InMemory::new());
    for key in KEYS {
        store.put(&key.into(), PutPayload::new()).await?;
    }

    // Call `list_with_depth` with `depth = 0`:
    let depth = 0;
    let prefix = None;
    let ListResult{objects, common_prefixes} = list_with_depth(&store, prefix, depth).await?;
    assert_eq!(objects[0].location, Path::from("a.txt"));
    assert_eq!(common_prefixes, vec![Path::from("foo")]);

    // Call `list_with_depth` with `depth = 1`:
    let depth = 1;
    let prefix = None;
    let ListResult{objects, common_prefixes} = list_with_depth(&store, prefix, depth).await?;
    assert_eq!(objects[0].location, Path::from("foo/b.txt"));
    assert_eq!(common_prefixes, vec![Path::from("foo/bar")]);

    Ok(())
}
```
