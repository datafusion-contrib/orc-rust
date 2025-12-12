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

from pathlib import Path

import pyorc
from datetime import date
from decimal import Decimal

OUT_PATH = Path(__file__).parent.parent / "tests" / "integration" / "data" / "bloom_filter.orc"


EXTRA_ROWS = 200


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    schema = (
        "struct<id:int,name:string,score:double,event_date:date,flag:boolean,data:binary,dec:decimal(10,2)>"
    )
    # Non-contiguous values make some predicates fall within min/max but absent
    # from the data (to exercise Bloom pruning). Include multiple rows to cover
    # richer value ranges.
    base_rows = [
        (1, "alpha", 1.0, (2023, 1, 1), True, b"\x01", Decimal("1.11")),
        (3, "gamma", 3.0, (2023, 1, 3), False, b"\x03", Decimal("3.33")),
        (5, "delta", 5.0, (2023, 1, 5), True, b"\x05", Decimal("5.55")),
        (10, "epsilon", 10.0, (2023, 1, 10), False, b"\x0a", Decimal("10.10")),
    ]

    # Add many more rows to create multiple row groups and stripes.
    # We deliberately skip certain even values (id=2, date=2023-01-02, binary 0x02, decimal 2.22)
    # so predicates for those values must rely on Bloom filters to prune.
    extra_rows = []
    day_choices = [1, 3, 4, 5, 6, 7]  # exclude day=2
    for i in range(EXTRA_ROWS):
        id_v = 101 + i * 2  # odd ids; still keeps id=2 absent
        name_v = f"name_{i}"
        score_v = float(id_v)
        day = day_choices[i % len(day_choices)]
        event_date = (2023, 1, day)
        flag = i % 2 == 0
        data = bytes([((i * 2) + 1) % 256])  # avoid byte 0x02
        dec = Decimal(f"{id_v}.01")
        extra_rows.append((id_v, name_v, score_v, event_date, flag, data, dec))

    rows = base_rows + extra_rows

    # Enable Bloom filters for all columns with a small false positive probability.
    with OUT_PATH.open("wb") as f:
        writer = pyorc.Writer(
            f,
            schema,
            bloom_filter_columns=[
                "id",
                "name",
                "score",
                "event_date",
                "flag",
                "data",
                "dec",
            ],
            bloom_filter_fpp=0.01,
            stripe_size=1024,
        )
        for id_v, name_v, score_v, (y, m, d), flag, data, dec in rows:
            writer.write((id_v, name_v, score_v, date(y, m, d), flag, data, dec))
        writer.close()

    print(f"Wrote ORC file with bloom filters to {OUT_PATH}")


if __name__ == "__main__":
    main()
