// Linux siginfo_t.si_code values for SIGSEGV.
// libc does not consistently export SEGV_* constants across targets/crate versions.
#[cfg(all(unix, target_os = "linux"))]
const _SEGV_MAPERR: libc::c_int = 1;
#[cfg(all(unix, target_os = "linux"))]
const _SEGV_ACCERR: libc::c_int = 2;

#[cfg(unix)]
extern "C" fn sigsegv_classifier(
    _sig: libc::c_int,
    info: *mut libc::siginfo_t,
    _uctx: *mut libc::c_void,
) {
    unsafe {
        let si_code = if info.is_null() { 0 } else { (*info).si_code };

        let mut msg: &[u8] = b"[fluxon_kv] SIGSEGV\n";
        #[cfg(target_os = "linux")]
        if si_code == _SEGV_ACCERR {
            msg = b"[fluxon_kv] SIGSEGV (SEGV_ACCERR): invalid permissions for mapped address (likely write to read-only mapping)\n";
        } else if si_code == _SEGV_MAPERR {
            msg = b"[fluxon_kv] SIGSEGV (SEGV_MAPERR): address not mapped\n";
        }
        #[cfg(not(target_os = "linux"))]
        if si_code != 0 {
            msg = b"[fluxon_kv] SIGSEGV: non-zero si_code (platform-specific)\n";
        }
        let _ = libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        libc::abort();
    }
}

#[cfg(unix)]
pub fn install_sigsegv_classifier() {
    unsafe {
        let mut new_action: libc::sigaction = std::mem::zeroed();

        new_action.sa_sigaction = sigsegv_classifier as libc::sighandler_t;
        new_action.sa_flags = libc::SA_SIGINFO;
        new_action.sa_restorer = None;

        let rc = libc::sigaction(libc::SIGSEGV, &new_action, std::ptr::null_mut());
        if rc != 0 {
            return;
        }
    }
}
