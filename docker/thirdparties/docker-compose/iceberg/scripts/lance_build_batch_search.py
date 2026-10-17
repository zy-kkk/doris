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

"""Build batch-search fixtures with two index segments and an uncovered tail.

Run with pylance 13.0.0b18 (segment-native index APIs) in an empty destination:
    python3 lance_build_batch_search.py preinstalled_data/lance
"""
import argparse
from pathlib import Path

import lance
import pyarrow as pa

DIMENSION = 8
FRAGMENT_ROWS = 256
INDEX_TYPES = {"batch_search": "IVF_FLAT", "batch_search_hnsw": "IVF_HNSW_SQ",
               "batch_search_hnsw_flat": "IVF_HNSW_FLAT"}


def rows(start, count):
    ids = list(range(start, start + count))
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "category": pa.array([value % 3 for value in ids], pa.int32()),
        "emb": pa.array([[float(value + col) for col in range(DIMENSION)] for value in ids],
                        pa.list_(pa.float32(), DIMENSION)),
        "payload": pa.array([f"row-{value}:" + "payload " * 128 for value in ids]),
    })


def build(root, tables):
    if lance.__version__ != "13.0.0-beta.18":
        raise RuntimeError("Batch fixture generation requires pylance 13.0.0b18")
    root = Path(root)
    for name in tables:
        index_type = INDEX_TYPES[name]
        uri = str(root / (name + ".lance"))
        if Path(uri).exists():
            raise FileExistsError(uri)
        for fragment in range(8):
            lance.write_dataset(rows(fragment * FRAGMENT_ROWS, FRAGMENT_ROWS), uri,
                                mode="create" if fragment == 0 else "append", data_storage_version="2.2")
        dataset = lance.dataset(uri)
        segments = [dataset.create_index_uncommitted("emb", index_type, name="emb_idx", metric="l2",
                                                      num_partitions=4, fragment_ids=list(range(start, start + 4)))
                    for start in (0, 4)]
        dataset.commit_existing_index_segments("emb_idx", "emb", segments)
        lance.write_dataset(rows(8 * FRAGMENT_ROWS, FRAGMENT_ROWS), uri, mode="append", data_storage_version="2.2")
        lance.dataset(uri).delete("id IN (42, 1042, 2200)")
    check(root, tables)


def check(root, tables):
    for name in tables:
        dataset = lance.dataset(str(Path(root) / (name + ".lance")))
        assert dataset.count_rows() == 9 * FRAGMENT_ROWS - 3
        assert len(dataset.get_fragments()) == 9
        indices = dataset.describe_indices()
        assert len(indices) == 1
        assert len(indices[0].segments) == 2
        assert sorted(sorted(segment.fragment_ids) for segment in indices[0].segments) == [list(range(4)), list(range(4, 8))]
        print(f"{name}: version={dataset.version}, fragments=9, segments=2, rows={dataset.count_rows()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--table", action="append", choices=tuple(INDEX_TYPES))
    args = parser.parse_args()
    tables = args.table or tuple(INDEX_TYPES)
    if args.check:
        check(args.root, tables)
    else:
        build(args.root, tables)
