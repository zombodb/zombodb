#ifndef __ZDBPOOL_H__
#define __ZDBPOOL_H__

#define ZDB_MAX_SHARDS 64
#define ZDB_MAX_REPLICAS 64
#define InvalidPoolIndex -1

typedef struct {
    bool   allocated[ZDB_MAX_SHARDS];
    uint32 start_at;
} ZDBIndexPoolEntry;

void zdb_pool_checkin(ZDBIndexDescriptor *desc);
void zdb_pool_checkout(ZDBIndexDescriptor *desc);

#endif