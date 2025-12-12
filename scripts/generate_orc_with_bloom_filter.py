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
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generate a small ORC file that contains Bloom filters for regression tests.

The generated file is written to:
  tests/integration/data/bloom_filter.orc

Dependencies:
  pip install pyorc

Usage:
  python scripts/generate_orc_with_bloom_filter.py
"""

import os
from pathlib import Path

import pyorc

OUT_PATH = Path(__file__).parent.parent / "tests" / "integration" / "data" / "bloom_filter.orc"


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    schema = "struct<id:int,name:string,score:double>"
    # Non-contiguous values make some predicates fall within min/max but absent
    # from the data (to exercise Bloom pruning). Include multiple rows to cover
    # positive hits as well.
    rows = [
        (1, "alpha", 1.0),
        (3, "gamma", 3.0),
        (5, "delta", 5.0),
        (10, "epsilon", 10.0),
    ]

    # Enable Bloom filters for all columns with a small false positive probability.
    with OUT_PATH.open("wb") as f:
        writer = pyorc.Writer(
            f,
            schema,
            bloom_filter_columns=["id", "name", "score"],
            bloom_filter_fpp=0.01,
            stripe_size=1024,
        )
        for row in rows:
            writer.write(row)
        writer.close()

    print(f"Wrote ORC file with bloom filters to {OUT_PATH}")


if __name__ == "__main__":
    main()
