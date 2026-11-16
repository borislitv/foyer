// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! File-device partition accounting through the public device API.

use foyer_storage::{DeviceBuilder, FileDeviceBuilder};

#[test]
fn file_partition_accounting_and_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let device = FileDeviceBuilder::new(dir.path().join("cache"))
        .with_capacity(7 * 4096)
        .build()
        .unwrap();
    assert_eq!(device.allocated(), 0);
    assert_eq!(device.free(), 7 * 4096);
    assert_eq!(device.partitions(), 0);

    // Unequal and zero-sized partitions must still be contiguous and ordered.
    for (id, size, offset, allocated) in [(0, 4096, 0, 4096), (1, 0, 4096, 4096), (2, 3 * 4096, 4096, 4 * 4096)] {
        let partition = device.create_partition(size).unwrap();
        assert_eq!(partition.id(), id);
        assert_eq!(partition.size(), size);
        assert_eq!(partition.translate(0).1, offset);
        assert_eq!(partition.translate(123).1, offset + 123);
        assert_eq!(device.partition(id).translate(0).1, offset);
        assert_eq!(device.partitions(), id as usize + 1);
        assert_eq!(device.allocated(), allocated);
        assert_eq!(device.free(), device.capacity() - allocated);
    }

    let error = device.create_partition(4 * 4096).unwrap_err();
    assert_eq!(error.kind(), foyer_common::error::ErrorKind::NoSpace);
    assert_eq!(device.partitions(), 3);
    assert_eq!(device.allocated(), 4 * 4096);
    assert_eq!(device.free(), 3 * 4096);

    let last = device.create_partition(3 * 4096).unwrap();
    assert_eq!(last.id(), 3);
    assert_eq!(last.translate(0).1, 4 * 4096);
    assert_eq!(device.allocated(), 7 * 4096);
    assert_eq!(device.free(), 0);
    assert!(device.create_partition(4096).is_err());
    assert_eq!(device.partitions(), 4);
    assert_eq!(device.partition(0).translate(0).1, 0);
    assert_eq!(device.partition(2).size(), 3 * 4096);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn file_partition_accounting_above_four_gib() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = (1usize << 32) + 4096;
    let device = FileDeviceBuilder::new(dir.path().join("cache"))
        .with_capacity(prefix + 4096)
        .build()
        .unwrap();
    device.create_partition(prefix).unwrap();
    assert_eq!(device.allocated(), prefix);
    assert_eq!(device.free(), 4096);
    let last = device.create_partition(4096).unwrap();
    assert_eq!(last.id(), 1);
    assert_eq!(last.translate(0).1, prefix as u64);
    assert_eq!(device.allocated(), prefix + 4096);
    assert_eq!(device.free(), 0);
}
