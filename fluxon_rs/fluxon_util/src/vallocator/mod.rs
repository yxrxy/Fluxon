mod frame;
pub use frame::*;

pub mod test;

use parking_lot::RwLock;

crate::define_error_code_enum_with_from! {
    #[repr(i32)]
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum AllocErrorCode {
        Success = 0,
        OutOfMemory = 1,
        InvalidSize = 2,
        InvalidPointer = 3,
        DoubleFree = 4,
        InvalidAddress = 5,
        NewException = 6,
        DeallocationException = 7,
        DeallocationUnknownException = 8,
        AllocationFailed = 9,
        AllocationException = 10,
        AllocationUnknownException = 11,
        SizeNotAligned = 12,
        InvalidCode = 10000000,
    }
    default: AllocErrorCode::InvalidCode
}

#[derive(Debug, Clone)]
pub struct AllocError {
    pub code: AllocErrorCode,
    pub message: String,
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllocError({:?}): {}", self.code, self.message)
    }
}

impl std::error::Error for AllocError {}

pub const ORDER: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocRegion {
    pub start_addr: u64,
    pub size: u64,
}

pub struct VirtualAllocator {
    inner: RwLock<FrameAllocator<ORDER>>,
}

impl std::fmt::Debug for VirtualAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let allocator = self.inner.read();
        f.debug_struct("VirtualAllocator")
            .field("total_size", &allocator.total)
            .field("allocated_size", &allocator.allocated)
            .finish()
    }
}

impl VirtualAllocator {
    pub fn new(total_size: u64) -> Result<Self, AllocError> {
        let mut allocator = FrameAllocator::<ORDER>::new();
        allocator.add_frame(0, total_size);

        Ok(Self {
            inner: RwLock::new(allocator),
        })
    }

    pub fn get_allocated_size(&self) -> u64 {
        self.inner.read().allocated
    }

    pub fn get_total_size(&self) -> u64 {
        self.inner.read().total
    }

    pub fn get_free_size(&self) -> u64 {
        let allocator = self.inner.read();
        allocator.total.saturating_sub(allocator.allocated)
    }

    pub fn alloc(&self, size: u64) -> Result<AllocRegion, AllocError> {
        if size == 0 {
            return Err(AllocError {
                code: AllocErrorCode::InvalidSize,
                message: format!("Invalid allocation size: {}", size),
            });
        }

        let mut allocator = self.inner.write();
        let (addr_opt, actual_size) = allocator.alloc(size);
        if let Some(addr) = addr_opt {
            if actual_size < size {
                return Err(AllocError {
                    code: AllocErrorCode::AllocationFailed,
                    message: format!(
                        "Allocated size {} is less than requested size {}",
                        actual_size, size
                    ),
                });
            }
            return Ok(AllocRegion {
                start_addr: addr,
                size: actual_size,
            });
        }

        Err(AllocError {
            code: AllocErrorCode::OutOfMemory,
            message: "Out of memory".to_string(),
        })
    }

    pub fn free(&self, ptr: u64, size: u64) -> Result<u64, AllocError> {
        if size == 0 {
            return Err(AllocError {
                code: AllocErrorCode::InvalidSize,
                message: format!("Invalid free size: {}", size),
            });
        }

        let mut allocator = self.inner.write();
        let freed_size = allocator.dealloc(ptr, size);
        if freed_size != size {
            return Err(AllocError {
                code: AllocErrorCode::DeallocationException,
                message: format!(
                    "Freed size {} is not equal to requested size {}",
                    freed_size, size
                ),
            });
        }
        Ok(freed_size)
    }
}
