# 1brc-rs

Solutions to [The One Billion Row
Challenge](https://www.morling.dev/blog/one-billion-row-challenge/), written in
Rust, with as few dependencies as possible.

To run the challenge, clone the [1brc
repo](https://github.com/gunnarmorling/1brc) and follow the instructions to
generate the input file. Then, compile this repo in release mode and run the
binary, specifying the version name and the input file.

```zsh
❯ cargo build --release
❯ time ./target/release/billion-rows v5 measurements.txt > output_v5.txt
./target/release/billion-rows v5 measurements.txt > output_v5.txt  47.46s user 6.46s system 676% cpu 7.975 total
```

## Solution versions

Each iteration on the solution is stored in a different file and can be selected
at runtime via a command-line arg. The following table summarizes each version,
describing the changes made from the previous version. Timings are from running on
my laptop, an Apple M1 Pro with 32GB of RAM.

| Version  | Time    | Description                                                                                                                          |
|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------|
| naive    | 2:23.33 | Straightforward implementation with no attempts at optimization.                                                                     |
| v2       | 2:25.24 | Read file using `mmap` instead of `read`.                                                                                            |
| v3       | 31.296  | Process file with multiple threads.                                                                                                  |
| v4       | 28.652  | Use [`fxhash`](https://github.com/cbreeden/fxhash) for `HashTable`s instead of the built-in (slower, but more secure) hash function. |
| v5       | 7.975   | Manual parser implementation that reduces allocations and skips utf8 checks.                                                         |

