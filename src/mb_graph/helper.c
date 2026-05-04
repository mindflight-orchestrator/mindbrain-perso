#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "executor/spi.h"

/* FunctionCallInfo argument helpers (flexible array member not accessible from Zig) */
Datum fcinfo_get_arg_value_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].value;
}

bool fcinfo_get_arg_isnull_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].isnull;
}

void fcinfo_set_isnull_helper(FunctionCallInfo fcinfo, bool isnull) {
    fcinfo->isnull = isnull;
}

/* elog wrapper (variadic macro cannot be translated by Zig) */
void elog_helper(int level, const char *msg) {
    elog(level, "%s", msg);
}

/* Varlena helpers (macros not reliably translated by Zig) */
struct varlena *detoast_datum_helper(Datum d) {
    return pg_detoast_datum((struct varlena *) DatumGetPointer(d));
}

int varhdrsz_helper(void) {
    return VARHDRSZ;
}

void set_varsize_helper(struct varlena *ptr, int size) {
    SET_VARSIZE(ptr, size);
}

/* Datum pointer conversion (PointerGetDatum macro) */
Datum pointer_get_datum_helper(const void *ptr) {
    return PointerGetDatum(ptr);
}

void *datum_get_pointer_helper(Datum d) {
    return DatumGetPointer(d);
}
