# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under this License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generate ORC files with Bloom Filters for testing.

This script generates a small ORC file with Bloom Filters enabled for testing.
The file size is kept small to be suitable for committing to the repository.

Usage:
    python scripts/generate_orc_with_bloom_filter.py
"""

import os
import sys

# Determine script directory and base directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)

# Use absolute path to ensure file is generated in correct location
dir = os.path.join(BASE_DIR, "tests", "integration", "data")
os.makedirs(dir, exist_ok=True)
output_file = os.path.join(dir, "bloom_filter_test.orc")

try:
    import pyarrow as pa
    import pyarrow.orc as orc

    print("Using PyArrow to generate ORC file with Bloom Filters...")
    
    # Generate a small test dataset (1000 rows) to keep file size small
    # This is enough to test Bloom Filter functionality while keeping the file small
    print("Generating test data (1000 rows)...")
    num_rows = 1000
    
    ids = list(range(1, num_rows + 1))
    names = [f"user_{i}" for i in ids]
    ages = [20 + (i % 50) for i in ids]  # Ages 20-69 (repeating)
    emails = [f"email_{i}@example.com" for i in ids]
    
    print("Creating PyArrow table...")
    table = pa.table({
        "id": pa.array(ids, type=pa.int32()),
        "name": pa.array(names, type=pa.string()),
        "age": pa.array(ages, type=pa.int32()),
        "email": pa.array(emails, type=pa.string()),
    })
    
    print("Writing ORC file with Bloom Filters...")
    print(f"  - Output file: {output_file}")
    print(f"  - Rows: {num_rows}")
    print(f"  - Bloom Filter columns: id (0), name (1), age (2), email (3)")
    print(f"  - False Positive Probability: 0.05")
    print(f"  - Row Index Stride: 500")
    
    # Write with Bloom Filter enabled for all columns
    # Note: PyArrow requires column indices, not names
    # Set row_index_stride to ensure row indexes are created (default is 10000)
    # For 1000 rows, we'll use 500 to create 2 row groups for better testing
    row_index_stride = 500
    orc.write_table(
        table,
        output_file,
        bloom_filter_columns=[0, 1, 2, 3],  # All columns: id, name, age, email
        bloom_filter_fpp=0.05,
        row_index_stride=row_index_stride,
    )
    
    # Check file size
    file_size = os.path.getsize(output_file)
    file_size_kb = file_size / 1024
    file_size_mb = file_size_kb / 1024
    
    print(f"\n✓ Successfully generated: {output_file}")
    print(f"  - Columns with Bloom Filter: id, name, age, email")
    print(f"  - Rows: {num_rows}")
    print(f"  - False Positive Probability: 0.05")
    print(f"  - Row Index Stride: {row_index_stride}")
    print(f"  - File size: {file_size:,} bytes ({file_size_kb:.2f} KB, {file_size_mb:.2f} MB)")
    
    if file_size_mb > 1:
        print(f"\n⚠ Warning: File size is {file_size_mb:.2f} MB, which may be large for repository")
    else:
        print(f"\n✓ File size is suitable for repository ({file_size_kb:.2f} KB)")
    
    print(f"\nYou can now run tests:")
    print(f"  cargo test --test integration test_bloom_filter")
    
except ImportError as e:
    print("ERROR: PyArrow is not available!")
    print(f"Import error: {e}")
    print("\nTo fix this, install PyArrow:")
    print("  pip install pyarrow")
    print("\nOr use the project venv:")
    print("  ./scripts/setup-venv.sh")
    print("  venv/bin/python scripts/generate_orc_with_bloom_filter.py")
    sys.exit(1)
except Exception as e:
    print(f"ERROR generating ORC file with PyArrow: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

