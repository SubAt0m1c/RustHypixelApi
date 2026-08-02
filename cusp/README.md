# cusp

cusp provides a lock-free, concurrent cell with atomic operations.

# Usage

Once you've created a cell, you can pin it and access or set the values.

```rust
use cusp::Cell;

let cell = Cell::new(0usize);

let pinned = cell.pin(); // pinned cell, bound to the lifetime of `cell`.

let value = pinned.get(); // &usize, bound to the lifetime of `pinned`.
assert_eq!(*value, 0);

pinned.set(1);
let new_value = pinned.get(); // this `get` now points to the newly set value.
assert_eq!(*new_value, 1);

// note `value` still refers to the value prior to the `set`. 
assert_eq!(*value, 0);
```

You can also perform complex atomic operations using `compute`.

```rust
use cusp::{Cell, Operation, Compute};

/// Gets the value of the cell if `Some`, otherwise sets it to `new` and returns `new`.
fn get_or_set(cell: &Cell<Option<usize>>, new: usize) -> usize {
    let pinned = cell.pin();
    let result = pinned.compute(|value| {
        match value {
            Some(_) => Operation::Abort(()), // abort will return a reference to the value seen in this closure.
            None => Operation::Set(Some(new)),
        }
    }); // `result` is bound to the lifetime of `pinned`, and functions like any other reference from a `get` or `swap` call.

    match result {
        Compute::Aborted { current, value: (), } => current.unwrap(), // this unwrap will always succeed, since we checked it in the closure.
        Compute::Set { old, new } => {
            assert!(old.is_none());
            new.unwrap() // this unwrap will always succeed, since it was set to `Some(new)`.
        }
    }
}
```
