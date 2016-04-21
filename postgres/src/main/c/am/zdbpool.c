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

    desc->current_pool_index = InvalidPoolIndex;
    while (true) {
        DirectFunctionCall1(pg_advisory_lock_int8, Int64GetDatum(desc->advisory_mutex));

        if (desc->shards == 1) {
            if (!entry->allocated[0]) {
                entry->allocated[0] = true;
                entry->start_at = 0;
                desc->current_pool_index = 0;
            }
        } else {
            int i;

            for (i = entry->start_at % desc->shards; i < desc->shards; i++) {
                if (!entry->allocated[i]) {
                    entry->allocated[i] = true;
                    entry->start_at++;
                    desc->current_pool_index = i;
                    break;
                }
            }
        }
        DirectFunctionCall1(pg_advisory_unlock_int8, Int64GetDatum(desc->advisory_mutex));

        if (desc->current_pool_index != InvalidPoolIndex)
            break;

        pg_usleep(100000);
    }
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