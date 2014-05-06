Highly Scalable No-blocking Hash Map in Rust
--------

The design of this hash map is taken and modified from [Dr. Cliff Click's design], [originally implemented in Java]. The hash map is thread-safe, thus can be safely used as a shared resource among multiple threads, without requiring mutual exclusion, and typesafe, because it is implemented in Rust. It is also scalable and can be shared among a large number of threads without experincing a major bottleneck. The work is currently under development.


## Design
The design of this hash map is based in the reasoning of a state machine, where a key-value pair is allowed to be in many valid states. To transfer from one state to another, the table requires an atomic [compare-and-swap] function to compare-and-swap pointers.
### State Machine
![][img]

Our design bases on the assumption that a key, once inserted, is never deleted directly from the table, but it can be cleaned up in a certian way.

To simplify things and make them easier to understand, let us start by assumming that we never need to resize the table. If it is the case, then only the `put_if_match` function can alter the states. If a key-value is inserted to the table, then the transition `{Empty, Empty} -> {Key, Empty} -> {Key, Value}` atomically occurs in the strict order, and only one thread can switch the state at a time (since we are using an atomic `compare-and-swap`.) If two threads are competing to update the table, say `Thread 1` just finishes the transition `{Empty, Empty} -> {Key, Empty}` and `Thread 2` suddenly shows up at `{Key, Empty}` and wants to update the same key, what would happen?

[Dr. Cliff Click's design]: https://www.youtube.com/watch?v=WYXgtXWejRM
[originally implemented in Java]: https://github.com/boundary/high-scale-lib/blob/master/src/main/java/org/cliffc/high_scale_lib/NonBlockingHashMap.java
[compare-and-swap]: http://en.wikipedia.org/wiki/Compare-and-swap
[img]: http://i.imgur.com/3VmE7Nl.jpg
