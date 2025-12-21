# ORC CLI Tool

A unified command-line tool for inspecting and exporting Apache ORC files.

## Installation

Build with the `cli` feature enabled:

```bash
cargo build --features cli --release
```

The binary will be available at `target/release/orc`.

## Usage

```bash
orc <COMMAND> [OPTIONS]
```

## Commands

### `info` - Display file metadata and schema

Display basic information about an ORC file including format version, compression, row count, and schema.

```bash
# Basic info
orc info file.orc

# Include stripe layout details
orc info --verbose file.orc

# Only show row counts (supports multiple files)
orc info --row-count-only file1.orc file2.orc
```

**Options:**
- `-v, --verbose` - Include stripe layout details (offsets, lengths, rows)
- `--row-count-only` - Only display the row count for each file

### `export` - Export data to CSV or JSON

Export ORC data to CSV or JSON format, with optional row limiting and column selection.

```bash
# Export to CSV (default)
orc export file.orc

# Export to JSON lines
orc export -f json file.orc

# Export first 100 rows
orc export -n 100 file.orc

# Export specific columns
orc export -c col1,col2,col3 file.orc

# Export to file
orc export -o output.csv file.orc

# Read from stdin
cat file.orc | orc export -

# Custom batch size
orc export --batch-size 4096 file.orc
```

**Options:**
- `-f, --format <FORMAT>` - Output format: `csv` (default) or `json`
- `-o, --output <FILE>` - Output file (default: stdout)
- `-n, --num-rows <N>` - Export only first N records (0 = all)
- `-c, --columns <COLS>` - Comma-separated list of columns to export
- `--batch-size <SIZE>` - Batch size for reading (default: 8192)

### `stats` - Print column and stripe statistics

Display detailed statistics for each column and stripe, including min/max values, null counts, and type-specific stats.

```bash
orc stats file.orc
```

**Output includes:**
- Column-level statistics (min, max, sum, null count)
- Per-stripe statistics
- Type-specific information (e.g., true_count for booleans)

### `layout` - Print physical layout as JSON

Output a JSON representation of the file's physical layout, useful for debugging and analysis.

```bash
orc layout file.orc
```

**JSON structure:**
```json
{
  "file": "path/to/file.orc",
  "format_version": "0.12",
  "compression": "ZLIB",
  "rows": 1000000,
  "stripes": [
    {
      "index": 0,
      "offset": 3,
      "index_length": 550,
      "data_length": 12345,
      "footer_length": 100,
      "rows": 10000,
      "streams": [...],
      "encodings": [...]
    }
  ]
}
```

### `index` - Print row group index information

Inspect row indexes for a specific column, useful for debugging predicate pushdown and verifying writer-produced indexes.

```bash
orc index file.orc column_name
```

**Output includes:**
- Column type information
- Per-stripe row group details
- Row group statistics (min, max, null count)

### `bloom` - Inspect bloom filters

Inspect bloom filters in ORC files. Bloom filters are probabilistic data structures that can quickly determine if a value is definitely NOT present in a row group, useful for predicate pushdown optimization.

```bash
# Show all columns with bloom filters
orc bloom file.orc

# Show bloom filter details for a specific column
orc bloom file.orc --column name

# Test if a value might exist in the bloom filter
orc bloom file.orc --column name --test "Alice"
```

**Options:**
- `-c, --column <NAME>` - Column name to inspect (show all if not specified)
- `-t, --test <VALUE>` - Test if a value might be contained in the bloom filter

**Output includes:**
- List of columns with bloom filters
- Number of row groups per column
- Hash function count and bit count per filter
- Test results showing if a value might be present (when `--test` is used)

## Examples

### Inspecting a file

```bash
# Quick overview
$ orc info data.orc
File: data.orc
Format version: 0.12
Compression: Zlib
Row index stride: 10000
Rows: 1000000
Stripes: 10

Schema:
ROOT
  id LONG
  name STRING
  timestamp TIMESTAMP

# Check multiple files
$ orc info --row-count-only *.orc
file1.orc: 1000000
file2.orc: 500000
file3.orc: 2000000
```

### Exporting data

```bash
# Preview first 10 rows as JSON
$ orc export -f json -n 10 data.orc
{"id":1,"name":"Alice","timestamp":"2024-01-01T00:00:00"}
{"id":2,"name":"Bob","timestamp":"2024-01-02T00:00:00"}
...

# Export specific columns to CSV file
$ orc export -c id,name -o users.csv data.orc
```

### Debugging

```bash
# Check column statistics
$ orc stats data.orc | grep -A5 "Column 1"
## Column 1
* Data type Integer
* Minimum: 1
* Maximum: 1000000
* Sum: 500000500000
* Num values: 1000000

# Analyze physical layout
$ orc layout data.orc | jq '.stripes[0].streams | length'
15

# Inspect row indexes for predicate pushdown
$ orc index data.orc id
File: data.orc | Column: id (index 1)
Type: LONG
Stripes: 10
Stripe 0: rows_per_group=10000 total_rows=100000
  Row group 0 rows [0,10000) -> values=10000, min=1, max=10000
  Row group 1 rows [10000,20000) -> values=10000, min=10001, max=20000
  ...

# Check bloom filters and test a value
$ orc bloom data.orc --column name --test "Alice"
File: data.orc
Stripes: 10

Columns with Bloom Filters:
  Column 2 (name): 100 row groups, 7 hash functions, 8192 bits/filter

Stripe 0:
  Column 2 (name):
    Row group 0: 128 words, 8192 bits, might_contain("Alice") = true
    Row group 1: 128 words, 8192 bits, might_contain("Alice") = false
    ...
```

## Migration from Legacy Commands

This unified `orc` tool replaces the following standalone commands:

| Legacy Command | New Command |
|---------------|-------------|
| `orc-metadata file.orc` | `orc info file.orc` |
| `orc-metadata -s file.orc` | `orc info --verbose file.orc` |
| `orc-schema file.orc` | `orc info file.orc` |
| `orc-schema -v file.orc` | `orc info --verbose file.orc` |
| `orc-rowcount file.orc` | `orc info --row-count-only file.orc` |
| `orc-export file.orc` | `orc export file.orc` |
| `orc-read file.orc` | `orc export file.orc` |
| `orc-read --json file.orc` | `orc export -f json file.orc` |
| `orc-stats file.orc` | `orc stats file.orc` |
| `orc-layout file.orc` | `orc layout file.orc` |
| `orc-index file.orc col` | `orc index file.orc col` |

