#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "executor/spi.h"
#include "access/htup_details.h"
#include "utils/typcache.h"
#include "access/tupmacs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/regproc.h"

Datum fcinfo_get_arg_value_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].value;
}

bool fcinfo_get_arg_isnull_helper(FunctionCallInfo fcinfo, int n) {
    return fcinfo->args[n].isnull;
}

void fcinfo_set_isnull_helper(FunctionCallInfo fcinfo, bool isnull) {
    fcinfo->isnull = isnull;
}

Datum srf_return_empty_helper(FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;

    if (rsi == NULL || !IsA(rsi, ReturnSetInfo))
        elog(ERROR, "pg_mindbrain: expected ReturnSetInfo");

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "pg_mindbrain: function must return a composite type");

    tupstore = tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random, false, 0);
    rsi->returnMode = SFRM_Materialize;
    rsi->setResult = tupstore;
    rsi->setDesc = tupdesc;
    return (Datum) 0;
}

void elog_helper(int level, const char *msg) {
    elog(level, "%s", msg);
}

struct varlena *detoast_datum_helper(Datum d) {
    return pg_detoast_datum((struct varlena *) DatumGetPointer(d));
}

int varsize_helper(struct varlena *ptr) {
    return VARSIZE(ptr);
}

char *vardata_helper(struct varlena *ptr) {
    return VARDATA(ptr);
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

int extract_facet_filter_fields(Datum composite_datum, Oid composite_type,
                                char **facet_name_out, int *facet_name_len,
                                char **facet_value_out, int *facet_value_len,
                                bool *facet_value_isnull) {
    HeapTupleHeader header = DatumGetHeapTupleHeader(composite_datum);
    TupleDesc tupdesc;
    bool isnull;
    Datum name_datum, value_datum;
    text *name_text, *value_text;
    HeapTupleData tuple;

    tupdesc = lookup_rowtype_tupdesc_copy(composite_type, -1);
    if (tupdesc == NULL) {
        return 0;
    }

    tuple.t_len = HeapTupleHeaderGetDatumLength(header);
    tuple.t_data = header;

    name_datum = fastgetattr(&tuple, 1, tupdesc, &isnull);
    if (isnull) {
        ReleaseTupleDesc(tupdesc);
        return 0;
    }
    name_text = DatumGetTextP(name_datum);
    *facet_name_out = VARDATA(name_text);
    *facet_name_len = VARSIZE(name_text) - VARHDRSZ;

    value_datum = fastgetattr(&tuple, 2, tupdesc, &isnull);
    *facet_value_isnull = isnull;
    if (!isnull) {
        value_text = DatumGetTextP(value_datum);
        *facet_value_out = VARDATA(value_text);
        *facet_value_len = VARSIZE(value_text) - VARHDRSZ;
    } else {
        *facet_value_out = NULL;
        *facet_value_len = 0;
    }

    ReleaseTupleDesc(tupdesc);
    return 1;
}

bool isa_helper(Node *node, NodeTag tag) {
    if (node == NULL) return false;
    return node->type == tag;
}

NodeTag t_returnsetinfo_helper(void) {
    return T_ReturnSetInfo;
}

int work_mem_helper(void) {
    extern int work_mem;
    return work_mem;
}

struct varlena *datum_get_textp_helper(Datum d) {
    return DatumGetTextP(d);
}

int varsize_any_exhdr_helper(struct varlena *ptr) {
    return VARSIZE_ANY_EXHDR(ptr);
}

char *vardata_any_helper(struct varlena *ptr) {
    return VARDATA_ANY(ptr);
}

char *text_to_cstring_helper(Datum d) {
    return text_to_cstring((text *) DatumGetTextP(d));
}

size_t strlen_helper(const char *str) {
    if (str == NULL) return 0;
    return strlen(str);
}

Datum text_datum_from_buf(const char *buf, size_t len) {
    return PointerGetDatum(cstring_to_text_with_len(buf, len));
}

void text_datum_to_slice(Datum d, const char **out_ptr, size_t *out_len) {
    struct varlena *v = DatumGetTextP(d);
    *out_ptr = VARDATA_ANY(v);
    *out_len = VARSIZE_ANY_EXHDR(v);
}

Oid get_ts_config_oid_helper(const char *config_name, bool missing_ok) {
    Datum datum = DirectFunctionCall1(regconfigin, CStringGetDatum(config_name));
    Oid oid = DatumGetObjectId(datum);

    if (oid == InvalidOid && !missing_ok) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("text search configuration \"%s\" does not exist", config_name)));
    }

    return oid;
}

Datum to_tsvector_byid_helper(Oid config_oid, Datum text_datum) {
    return DirectFunctionCall2(to_tsvector_byid, ObjectIdGetDatum(config_oid), text_datum);
}

#if !defined(__x86_64__) && !defined(_M_AMD64)
__attribute__((weak)) int croaring_hardware_support(void) {
    return 0;
}
#endif
