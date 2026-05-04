#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/tuplestore.h"
#include "executor/spi.h"

/* Return empty set from a set-returning function. Uses get_call_result_type for tuple descriptor. */
Datum srf_return_empty_helper(FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;

    if (rsi == NULL || !IsA(rsi, ReturnSetInfo))
        elog(ERROR, "pg_pragma: expected ReturnSetInfo");

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "pg_pragma: function must return a composite type");

    tupstore = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, 0);
    rsi->returnMode = SFRM_Materialize;
    rsi->setResult = tupstore;
    rsi->setDesc = tupdesc;
    return (Datum) 0;
}

Datum fcinfo_get_arg_value_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].value;
}

bool fcinfo_get_arg_isnull_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].isnull;
}

void fcinfo_set_isnull_helper(FunctionCallInfo fcinfo, bool isnull) {
    fcinfo->isnull = isnull;
}

void elog_helper(int level, const char *msg) {
    elog(level, "%s", msg);
}

struct varlena *detoast_datum_helper(Datum d) {
    return pg_detoast_datum((struct varlena *) DatumGetPointer(d));
}

int varhdrsz_helper(void) {
    return VARHDRSZ;
}

void set_varsize_helper(struct varlena *ptr, int size) {
    SET_VARSIZE(ptr, size);
}

Datum pointer_get_datum_helper(const void *ptr) {
    return PointerGetDatum(ptr);
}

/* Build a text Datum from a buffer (palloc'd, null-terminated). Caller must not free. */
Datum text_datum_from_buf(const char *buf, size_t len) {
    return PointerGetDatum(cstring_to_text_with_len(buf, len));
}

/* Get pointer and length from a text Datum (detoasted). */
void text_datum_to_slice(Datum d, const char **out_ptr, size_t *out_len) {
    struct varlena *v = DatumGetTextP(d);
    *out_ptr = VARDATA_ANY(v);
    *out_len = VARSIZE_ANY_EXHDR(v);
}
