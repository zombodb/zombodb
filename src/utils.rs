use pgx::*;

#[inline]
pub fn convert_xid(xid: pg_sys::TransactionId) -> u64 {
    let mut last_xid = pg_sys::InvalidTransactionId;
    let mut epoch = 0u32;

    unsafe {
        pg_sys::GetNextXidAndEpoch(&mut last_xid, &mut epoch);
    }

    /* return special xid's as-is */
    if !pg_sys::TransactionIdIsNormal(xid) {
        return xid as u64;
    }

    /* xid can be on either side when near wrap-around */
    let mut epoch = epoch as u64;
    if xid > last_xid && unsafe { pg_sys::TransactionIdPrecedes(xid, last_xid) } {
        epoch -= 1;
    } else if xid < last_xid && unsafe { pg_sys::TransactionIdFollows(xid, last_xid) } {
        epoch += 1;
    }

    return (epoch << 32) | xid as u64;
}
