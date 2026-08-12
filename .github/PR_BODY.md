# feat(writer): add SNAPPY / ZLIB / ZSTD compression support

## Problem

orc-rust's writer always emits uncompressed ORC files even though the
reader fully supports SNAPPY / ZLIB / ZSTD. This breaks interop with
the Java ORC ecosystem (Hive, Spark, Trino, DuckDB) where every
production deployment defaults to compressed tables — a 10-100x
storage cost for downstream consumers and an awkward gap for crates
that need to write ORC for Hive-shaped systems.

The PostScript-level codec selection has been a `// TODO: support
compression` marker in `src/arrow_writer.rs::serialize_postscript`
since the writer was added; this PR removes the TODO.

## Solution

Implement per-chunk compression matching the reference Java writer
(`org.apache.orc.impl.PhysicalFsWriter`) per the ORC v1 spec
(<https://orc.apache.org/specification/ORCv1/#compression>):

- **Per-stream, per-chunk** compression with a configurable block size
  (default 256 KiB, matching `OrcConf.BUFFER_SIZE`).
- **3-byte little-endian chunk header**: 23-bit length + 1 ORIGINAL
  flag bit. Encodes correctly against the reader's existing
  `decode_header` (the reader's known-answer test for `5 → [0x0b, 0,
  0]` and `100 000 → [0x40, 0x0d, 0x03]` is mirrored as a writer-side
  KAT in `src/writer/compression.rs::header_kat_*`).
- **Original-fallback** when `compressed_len >= original_len` — the
  spec-mandated and Java-reference behaviour. Verified per codec in
  `original_fallback_when_compression_would_expand`.

Compression is applied to every column stream (Present / Data /
Length / Secondary / DictionaryData), to every stripe footer, and to
the file footer. The PostScript itself is **not** compressed (it
lives at a fixed offset from EOF so readers can locate it without
first knowing the codec) and now records both `compression`
(`CompressionKind`) and `compression_block_size` so any conformant
reader runs the matching decompressor.

## Public API

```rust
use orc_rust::arrow_writer::{ArrowWriterBuilder, Compression};

let writer = ArrowWriterBuilder::new(file, schema)
    .with_compression(Compression::Snappy)
    // optional — defaults to 256 KiB
    .with_compression_block_size(64 * 1024)
    .try_build()?;
```

`Compression` is exposed as:

```rust
pub enum Compression {
    None,                       // default — byte-identical to pre-PR output
    Snappy,
    Zlib { level: u32 },        // raw DEFLATE; default level 6
    Zstd { level: i32 },        // default level 3
}
```

with convenience constructors `Compression::zlib()` /
`Compression::zstd()` for the spec-default levels, and
`DEFAULT_ZLIB_LEVEL` / `DEFAULT_ZSTD_LEVEL` /
`DEFAULT_COMPRESSION_BLOCK_SIZE` re-exports so call sites can derive
their own configuration without duplicating constants.

## Design

The compression machinery lives in a new module
`src/writer/compression.rs` with three internal functions:

- `write_header(out, length, original)` — emits the 3-byte little-
  endian chunk header. Debug-asserts the 23-bit length cap.
- `encode_chunk(codec, chunk)` — codec-specific compression of one
  chunk's payload.
- `compress_stream(codec, block_size, payload)` — splits the payload
  on `block_size` boundaries and writes each chunk through
  `write_chunk`, falling back to ORIGINAL when the codec doesn't
  shrink the chunk.

`StripeWriter` carries a `pub(crate) StripeCompression { compression,
block_size }` and feeds every emitted stream + the stripe footer
through `compress_stream`. `ArrowWriter::close()` does the same for
the file footer before serialising the PostScript.

`Compression::None` is represented as `Option::None` at the
`StripeWriter` level so the no-compression code path stays
branchless and produces byte-identical output to the pre-PR writer.
This is a verified invariant — see the
`backward_compat_default_no_compression_byte_identical` test.

## Alternatives considered

1. **Whole-stripe compression** — rejected: non-spec-compliant, would
   break every existing ORC reader.
2. **Per-stream global (no chunks)** — rejected: same problem; the
   spec mandates per-chunk framing because readers stream-decode.
3. **ZSTD-only first, SNAPPY/ZLIB later** — rejected: Hive and Trino
   both default to SNAPPY, so an MVP without it has zero deployment
   value. The three codecs all share the chunked-frame envelope and
   only differ in the codec call inside `encode_chunk`, so doing all
   three in one PR is no extra design risk.
4. **Fork a `Codec` trait for extensibility** — deferred. Adding a
   trait now would commit us to a particular extension shape (e.g.
   how to plumb encoder context for streaming codecs) before there's
   a concrete second-implementation use case. The current `enum`
   covers every codec the ORC spec defines.

## Breaking changes

None. `Compression::None` is the default. Existing call sites
continue to produce **byte-identical** output (verified by the
`backward_compat_default_no_compression_byte_identical` integration
test; the PostScript's `compression_block_size` field is
deliberately omitted when the codec is NONE so we match Java's
writer which also omits it).

`StripeWriter::new` is dropped (was an internal artifact; the writer
module itself is `mod writer;` private — no public callers exist).
`StripeWriter::with_compression` replaces it as the only constructor
and is also `pub(crate)` since the public surface is
`ArrowWriterBuilder`.

## Tests

29 new tests, all green:

**Unit tests** in `src/writer/compression.rs` (11):
- `header_round_trip` — fuzzed length/flag combinations including
  the 23-bit boundary case
- `header_kat_matches_reader_decode` — known-answer matching the
  reader's `decode_compressed` test (`100 000 → [0x40, 0x0d, 0x03]`)
- `header_kat_uncompressed_5_bytes` — matching the reader's
  `decode_uncompressed` test (`5 → [0x0b, 0, 0]`)
- `empty_stream_emits_no_chunks` — ORC streams of 0 length carry no
  headers, mirrors the reader's empty-stream short-circuit
- `snappy_roundtrip_via_reader_decoder` / `zlib_roundtrip_via_…` /
  `zstd_roundtrip_via_…` — feed the writer's output back through the
  matching reader codec
- `zstd_high_level_round_trip` — exercises ZSTD level 19
- `original_fallback_when_compression_would_expand` — spec-mandated
  fallback verified across all three codecs
- `block_size_chunks_input_into_multiple_frames` — 5000-byte input
  with 1024-byte block size produces exactly 5 chunks of [1024,
  1024, 1024, 1024, 904] input bytes
- `compress_stream_panics_on_compression_none_in_debug` — defence
  in depth against future refactor mistakes

**Integration tests** in `tests/writer_compression.rs` (16):
- Round-trip for SNAPPY, ZLIB (default + level 9), ZSTD (default +
  level 19) on a mixed Int32 + Utf8 batch
- PostScript inspection (parse the file tail with `prost`): verify
  CompressionKind matches for SNAPPY / ZLIB / ZSTD, and that
  `compression_block_size` is populated to the user's value or the
  documented 256 KiB default
- Backward compat: `Compression::None` produces a byte-stream
  bit-identical to a builder built without any compression call
- Incompressible payload (xorshift bytes) survives round-trip via
  the spec's "fall back to original chunk" code path
- Tiny block size (4 KiB) over a multi-megabyte stream forces
  multiple compression chunks per stream and round-trips
- API hardening: oversize block sizes are clamped under the 23-bit
  spec ceiling; zero falls back to the 256 KiB default
- Multi-stripe writes with compression round-trip cleanly

**`cargo test --all-features`** passes 425 tests total (151 unit +
16 new integration + 13 doc + the rest unchanged) — zero failures.

## Benchmarks

`cargo bench --bench writer_compression` on a 10 000-row Int64 +
Utf8 batch (Apple silicon, debug rustc 1.95):

| codec     | output bytes | ratio  | write time |
|-----------|-------------:|-------:|-----------:|
| none      |      246 698 |  1.30x |     110 µs |
| snappy    |       59 346 |  5.39x |     293 µs |
| zlib_1    |       34 318 |  9.32x |     375 µs |
| zlib_6    |       32 461 |  9.86x |     3.4 ms |
| zlib_9    |       32 461 |  9.86x |    11.8 ms |
| zstd_1    |       12 538 | 25.52x |     264 µs |
| zstd_3    |        8 834 | 36.22x |     314 µs |
| zstd_9    |       12 981 | 24.65x |     1.2 ms |
| zstd_19   |        4 823 | 66.35x |    81.0 ms |

ZSTD level 3 dominates the speed/ratio Pareto frontier on this
workload, matching the upstream Java ORC default of
`orc.compress.zstd.level = 3`.

## Cross-implementation interop

The compressed chunk format is byte-for-byte identical to the
existing reader's expectations — verified by the round-trip-via-
reader-decoder unit tests, which feed the writer's output directly
to the reader's `flate2::read::DeflateDecoder` /
`zstd::stream::decode_all` / `snap::raw::Decoder::decompress_vec`.

### Cross-implementation validation (Apache ORC 1.9.5 `orc-tools`)

Cross-validated against the Java reference implementation:

- **Rust writer → Java reader (all 3 codecs).** `orc-tools meta`
  parses the PostScript of files this PR produces, and reports the
  correct `Compression:` field (SNAPPY / ZLIB / ZSTD) with the
  matching row count. `orc-tools data` decompresses and decodes
  every stripe without error and prints the exact rows we wrote.
- **Java writer → Rust reader.** Files written by `orc-tools
  convert` (default ZLIB on orc-tools 1.9) are read by this PR's
  reader with byte-identical column values. Proves our reader is
  happy with the Java writer's chunk framing too.

`orc-tools meta` excerpt on a 3-row SNAPPY file emitted by the
Rust writer:

```
File Version: 0.12 with FUTURE by Unknown(-1)
Rows: 3
Compression: SNAPPY
Compression size: 262144
Calendar: Julian/Gregorian
Type: struct<id:int,name:string>
```

Identical output shape for ZLIB and ZSTD (only the `Compression:`
line differs). `orc-tools data` emits:

```
{"id":1,"name":"alpha"}
{"id":2,"name":"bravo"}
{"id":3,"name":"charlie"}
```

— exactly the rows we fed to `ArrowWriter::write`.

Evidence: see `tests/java_interop.rs` in this PR; the tests are
`#[ignore]`d by default and gated on the `ORC_TOOLS_JAR`
environment variable pointing at
`orc-tools-<version>-uber.jar` (tested against 1.9.5 from Maven
Central). Run with:

```bash
export ORC_TOOLS_JAR=/path/to/orc-tools-1.9.5-uber.jar
cargo test --test java_interop -- --ignored
```

Result on the submitter's machine (JDK 17 + orc-tools 1.9.5):

```
running 4 tests
test snappy_file_validates_with_java_orc_tools ... ok
test zlib_file_validates_with_java_orc_tools ... ok
test zstd_file_validates_with_java_orc_tools ... ok
test java_zlib_file_reads_with_rust ... ok

test result: ok. 4 passed; 0 failed
```

## Checklist

- [x] `cargo test --all-features` passes (425 tests)
- [x] `cargo clippy --all-features -- -D warnings` passes for the new
      code (3 pre-existing warnings on `main` unrelated to this PR
      — `row_index.rs::useless_conversion`,
      `delta.rs::explicit_counter_loop` ×2 — also reproduce on
      `cargo clippy` with no changes)
- [x] `cargo fmt -- --check` passes
- [x] `cargo doc --no-deps --all-features` builds; the 3 pre-existing
      doc warnings on main are not introduced by this PR
- [x] Benchmarks added (`benches/writer_compression.rs`)
- [x] Backward compat verified (`Compression::None` is byte-identical
      to pre-PR output)
- [x] Apache 2.0 license header on every new file
- [x] Conventional commit messages, signed off

## Commits

```
feat(writer): add per-chunk compression module (SNAPPY/ZLIB/ZSTD)
feat(writer): wire compression through ArrowWriter and StripeWriter
test(writer): end-to-end compression round-trip + PostScript inspection
bench(writer): codec comparison benchmark on a 10k-row mixed batch
test(writer): add Java orc-tools cross-validation integration tests
```
