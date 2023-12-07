# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:
1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go, Scala, or Rust would be preferred. You may use any data store of your choosing, with `sqlite` being one example.

--- 

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account.  This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.


## Write-up

You need to have [Rust](https://www.rust-lang.org/tools/install) installed. 

Run `sudo apt-get install libsqlite3-dev && cd processor && cargo run`. If you are on Windows, please run on WSL. If on mac, please use Brew to install sqlite3. This will take ~2 minutes to install sqlite and build initially. I'd put this all into a Dockerfile with all the fun caching and then the Docker Compose file, for all the best practices and ease of running, but I'll elide that for now.

For tests run `export RUST_TEST_THREADS=1; cargo test`. You will see tests in the processor/src/main.rs file. I separated the connection and ack/nack from processing individual datums to support testing. This tests overwriting, duplicates, out of order and both data versions.

Considerations included how to handle reordering wrt timestamp, duplication, time in distributed systems, database choice, data loss...

### Implementation

I processed messages one at a time and upserted them in sqlite. 

To tolerate duplication or out-or-order messages, I ensured that the upsert made timestamps monotonically increase. Out-of-order and duplications correspond to decreases or strict non-increases in timestamps.

Better throughput could be achieved by handling multiple messages at once, or using a local lightweight KV store like RocksDB (with the usage of their Merge operator to "upsert").

A storage instance that supports multiple writers will be needed to enable horizontal scaling. Though, at some point, this instance will also need to horizontally scale. If we can tolerate eventual or causal consistency, this instance can be more easily scaled.

### Time

We decide to overwrite based on timestamp, depending on the scanner. If two scanners add the same host with different data at the same time, who wins? The way it is handled now is whatever get's processed first. 

I think the best way to handle this is to ensure the internet is partitioned for the scanners. Yet, reforming partitions presents additional complexity to handle. "Best-effort" may be fine, depending on downstream applications.

### Preventing data loss

The delivery semantics are at-least once and we only ack if it has successfully been written in the database. So, the only way some data loss can happen is in the message broker or in the sqlite instance (barring large consumer queues, see extensions). If enough replication was added, data loss rates should be within our SLOs.

### Extensions

There are numerous way to extend this. We could expose richer use cases downstream by storing data in windows, so downstream application can see data versioned at a particular time.

In case upstream producers change versions frequently, we could use a schema migration tool or code up separate a lightweight adaptor function.

Error handling can be improved. Retries, dead letter queues, circuit breaking could be implemented. Metrics on failure, or queue size can help production support with adding more consumer instances or diagnosing what is broken.

On migrations and error handling: Rust makes it easier to ensure all possible cases are handled and that error handling is explicit and can be properly bubbled up.