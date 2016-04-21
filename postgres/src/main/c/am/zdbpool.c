#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

#include "zdb_interface.h"
#include "zdbpool.h"

static ZDBIndexPoolEntry *zdb_pool_get_entry(ZDBIndexDescriptor *desc) {
    ZDBIndexPoolEntry *entry;
    bool found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    entry = (ZDBIndexPoolEntry *) ShmemInitStruct(desc->fullyQualifiedName, sizeof(ZDBIndexPoolEntry), &found);
    if (!found)
        memset(entry, 0, sizeof(ZDBIndexPoolEntry));
    LWLockRelease(AddinShmemInitLock);

    return entry;
}

void zdb_pool_checkout(ZDBIndexDescriptor *desc) {
    ZDBIndexPoolEntry *entry = zdb_pool_get_entry(desc);
    int i;
    bool got_it = false;

    DirectFunctionCall1(pg_advisory_lock_int8, Int64GetDatum(desc->advisory_mutex));
    for (i=entry->start_at%desc->shards; i<desc->shards; i++) {
        if (!entry->allocated[i]) {
            entry->allocated[i] = true;
            entry->start_at++;
            got_it = true;
            break;
        }
    }
    DirectFunctionCall1(pg_advisory_unlock_int8, Int64GetDatum(desc->advisory_mutex));

    if (!got_it) {
        // TODO:  how to wait and retry?
        elog(ERROR, "No pool entries available");
    }

    desc->current_pool_index = i;
}

void zdb_pool_checkin(ZDBIndexDescriptor *desc) {
    if (desc->current_pool_index != InvalidPoolIndex) {
        ZDBIndexPoolEntry *entry = zdb_pool_get_entry(desc);

        DirectFunctionCall1(pg_advisory_lock_int8, Int64GetDatum(desc->advisory_mutex));
        entry->allocated[desc->current_pool_index] = false;
        DirectFunctionCall1(pg_advisory_unlock_int8, Int64GetDatum(desc->advisory_mutex));

        desc->current_pool_index = InvalidPoolIndex;
    }
}