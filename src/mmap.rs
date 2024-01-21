use std::{fs::File, os::fd::IntoRawFd, ptr, slice};

use anyhow::{bail, Result};
use libc::c_void;

pub struct MMappedFile<'a> {
    ptr: *mut c_void,
    size: usize,
    data: &'a [u8],
}

impl<'a> MMappedFile<'a> {
    pub unsafe fn new(file: File) -> Result<Self> {
        let metadata = file.metadata()?;
        let size = metadata.len() as usize;
        let fd = file.into_raw_fd();
        let ptr = libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            fd,
            0,
        );
        if ptr == libc::MAP_FAILED {
            bail!("failed to mmap file")
        }
        let data = slice::from_raw_parts(ptr as *const u8, size);

        Ok(MMappedFile { ptr, size, data })
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.data
    }
}

impl<'a> Drop for MMappedFile<'a> {
    fn drop(&mut self) {
        let ret = unsafe { libc::munmap(self.ptr, self.size) };
        if ret != 0 {
            panic!("munmap failed");
        }
    }
}
