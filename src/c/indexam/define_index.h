//
// Created by Eric B. Ridge on 2019-02-23.
//

#ifndef ZOMBODB_DEFINE_INDEX_H
#define ZOMBODB_DEFINE_INDEX_H

#include "postgres.h"
#include "catalog/objectaddress.h"
#include "zombodb.h"

ObjectAddress
zdbDefineIndex(Oid relationId,
               IndexStmt *stmt,
               Oid indexRelationId,
               Oid parentIndexId,
               Oid parentConstraintId,
               bool is_alter_table,
               bool check_rights,
               bool check_not_in_use,
               bool skip_build,
               bool quiet);

#endif //ZOMBODB_DEFINE_INDEX_H
