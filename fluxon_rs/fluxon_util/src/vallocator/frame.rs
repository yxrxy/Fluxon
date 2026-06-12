use core::alloc::Layout;
use core::cmp::{max, min};
use core::ops::Range;
use std::collections::BTreeSet;

fn prev_power_of_two(num: u64) -> u64 {
    1 << (u64::BITS as u64 - num.leading_zeros() as u64 - 1)
}

pub struct FrameAllocator<const ORDER: usize = 33> {
    // Buddy system with max order of `ORDER - 1`.
    free_list: [BTreeSet<u64>; ORDER],

    // Statistics.
    pub allocated: u64,
    pub total: u64,
}

impl<const ORDER: usize> FrameAllocator<ORDER> {
    pub const fn new() -> Self {
        Self {
            free_list: [const { BTreeSet::new() }; ORDER],
            allocated: 0,
            total: 0,
        }
    }

    pub fn add_frame(&mut self, start: u64, end: u64) {
        assert!(start <= end);

        let mut total = 0;
        let mut current_start = start;

        while current_start < end {
            let lowbit = if current_start > 0 {
                current_start & (!current_start + 1)
            } else {
                1 << (ORDER - 1)
            };
            let size = min(
                min(lowbit, prev_power_of_two(end - current_start)),
                1 << (ORDER - 1),
            );
            total += size;

            self.free_list[size.trailing_zeros() as usize].insert(current_start);
            current_start += size;
        }

        self.total += total;
    }

    pub fn insert(&mut self, range: Range<u64>) {
        self.add_frame(range.start, range.end);
    }

    pub fn alloc(&mut self, count: u64) -> (Option<u64>, u64) {
        let size = count.next_power_of_two();
        self.alloc_power_of_two(size)
    }

    pub fn alloc_aligned(&mut self, layout: Layout) -> (Option<u64>, u64) {
        let size = max(layout.size().next_power_of_two(), layout.align());
        self.alloc_power_of_two(size as u64)
    }

    fn alloc_power_of_two(&mut self, size: u64) -> (Option<u64>, u64) {
        let class = size.trailing_zeros() as usize;
        for i in class..self.free_list.len() {
            if !self.free_list[i].is_empty() {
                for j in (class + 1..i + 1).rev() {
                    if let Some(block_ref) = self.free_list[j].iter().next() {
                        let block = *block_ref;
                        self.free_list[j - 1].insert(block + (1 << (j - 1)));
                        self.free_list[j - 1].insert(block);
                        self.free_list[j].remove(&block);
                    } else {
                        return (None, 0);
                    }
                }

                let result = self.free_list[class].iter().next();
                if let Some(result_ref) = result {
                    let result = *result_ref;
                    self.free_list[class].remove(&result);
                    self.allocated += size;
                    return (Some(result), size);
                } else {
                    return (None, 0);
                }
            }
        }
        (None, 0)
    }

    pub fn dealloc(&mut self, start_frame: u64, count: u64) -> u64 {
        let size = count.next_power_of_two();
        self.dealloc_power_of_two(start_frame, size)
    }

    pub fn dealloc_aligned(&mut self, start_frame: u64, layout: Layout) -> u64 {
        let size = max(layout.size().next_power_of_two(), layout.align());
        self.dealloc_power_of_two(start_frame, size as u64)
    }

    fn dealloc_power_of_two(&mut self, start_frame: u64, size: u64) -> u64 {
        let class = size.trailing_zeros() as usize;

        let mut current_ptr = start_frame;
        let mut current_class = class;
        while current_class < self.free_list.len() {
            let buddy = current_ptr ^ (1 << current_class);
            if self.free_list[current_class].remove(&buddy) {
                current_ptr = min(current_ptr, buddy);
                current_class += 1;
            } else {
                self.free_list[current_class].insert(current_ptr);
                break;
            }
        }

        self.allocated -= size;
        size
    }
}
