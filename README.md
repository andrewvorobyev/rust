## Foreword

This is my first `Rust` program (beyond hello world of course!). I skimmed [The Book](https://doc.rust-lang.org/book/title-page.html) and [Async Book](https://rust-lang.github.io/async-book/01_getting_started/04_async_await_primer.html) last week. That's the only `Rust` knowledge I have at the moment. At the same time I'm very proficient in `Scala` and `C++` which helped ramp up a lot. That being said, expect some inefficiencies and newbie mistakes here and there in the code.

## Performance & Assumptions

The program runs utilizing all available CPU cores by partitioning transactions between worker threads. It streams transactions while processing. The main bottleneck is the memory due to each partition storing the entire history of transactions it has processed. This is necessary to resolve transaction references in the dispute handling. There are ways to reduce the memory pressure but it will require tradeoffs (e.g. limiting the history, storing the externally etc.).

## Testing

I mostly relied on unit tests in `lib.rs`. Did a manual run as well to test. It would be worth doing perf testing with large inputs but I skipped that. It may also be worth adding some more unit tests.
