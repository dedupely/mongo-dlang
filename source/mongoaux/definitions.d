module mongoaux.definitions;

public import mongoaux.mongoc;
alias ssize_t = ptrdiff_t;
extern (C) {
  // TODO: Change for example ints to C ones.
  char * bson_as_canonical_extended_json(const bson_t *bson, size_t *length);
  void bson_init(bson_t * b);
  bson_t * bson_new();
  bson_t * bson_copy(const bson_t * bson);
  void bson_free(void * mem);
  bool mongoc_collection_delete_one(
    mongoc_collection_t *collection,
    const bson_t *selector,
    const bson_t *opts,
    bson_t *reply,
    bson_error_t *error
  );
  bool mongoc_collection_insert_one(
    mongoc_collection_t *collection,
    const bson_t *document,
    const bson_t *opts,
    bson_t *reply,
    bson_error_t *error
  );
  bool mongoc_collection_update_one(
    mongoc_collection_t *collection,
    const bson_t *selector,
    const bson_t *update,
    const bson_t *opts,
    bson_t *reply,
    bson_error_t *error
  );
  bool bson_init_from_json(
    bson_t *bson,
    const char *data,
    ssize_t len,
    bson_error_t *error
  );
  bool bson_has_field(const bson_t *bson, const char *key);
  bool bson_append_int32(
    bson_t *bson,
    const char *key,
    int key_length,
    int value
  );
  bool bson_append_int64(
    bson_t *bson,
    const char *key,
    int key_length,
    long value
  );
  bool bson_append_bool(
    bson_t *bson,
    const char *key,
    int key_length,
    bool value
  );
  bool bson_append_double(
    bson_t *bson,
    const char *key,
    int key_length,
    double value
  );
  bool bson_append_utf8(
    bson_t *bson,
    const char *key,
    int key_length,
    const char *value,
    int length
  );
  bool bson_append_date_time(
    bson_t *bson,
    const char *key,
    int key_length,
    long value
  );
  bool bson_append_array_begin(
    bson_t *bson,
    const char *key,
    int key_length,
    bson_t *child
  );
  bool bson_append_array_end(
    bson_t *bson,
    bson_t *child
  );
  bool bson_append_document_begin(
    bson_t *bson,
    const char *key,
    int key_length,
    bson_t *child
  );
  bool bson_append_document_end (bson_t *bson, bson_t *child);
  bool bson_append_document(
    bson_t *bson,
    const char *key,
    int key_length,
    const bson_t *value
  );
  bool bson_append_oid(
    bson_t *bson,
    const char *key,
    int key_length,
    const bson_oid_t *oid
  );
  mongoc_bulk_operation_t * mongoc_collection_create_bulk_operation_with_opts(
   mongoc_collection_t *collection,
   const bson_t *opts
  );
  bool mongoc_collection_insert_many(
    mongoc_collection_t *collection,
    const bson_t **documents,
    size_t n_documents,
    const bson_t *opts,
    bson_t *reply,
    bson_error_t *error
  );
  // These 3 struct definitions are taken from mongoc.d
  // They're marked IGN there, don't know why.
  struct _bson_value_t {
    bson_type_t value_type;
    int padding;

    union _Anonymous_0
    {
      bson_oid_t v_oid;
      long v_int64;
      int v_int32;
      byte v_int8;
      double v_double;
      bool v_bool;
      long v_datetime;

      struct _Anonymous_1
      {
        uint timestamp;
        uint increment;
      }

      _Anonymous_1 v_timestamp;

      struct _Anonymous_2
      {
        char* str;
        uint len;
      }

      _Anonymous_2 v_utf8;

      struct _Anonymous_3
      {
        ubyte* data;
        uint data_len;
      }

      _Anonymous_3 v_doc;

      struct _Anonymous_4
      {
        ubyte* data;
        uint data_len;
        bson_subtype_t subtype;
      }

      _Anonymous_4 v_binary;

      struct _Anonymous_5
      {
        char* regex;
        char* options;
      }

      _Anonymous_5 v_regex;

      struct _Anonymous_6
      {
        char* collection;
        uint collection_len;
        bson_oid_t oid;
      }

      _Anonymous_6 v_dbpointer;

      struct _Anonymous_7
      {
        char* code;
        uint code_len;
      }

      _Anonymous_7 v_code;

      struct _Anonymous_8
      {
        char* code;
        ubyte* scope_data;
        uint code_len;
        uint scope_len;
      }

      _Anonymous_8 v_codewscope;

      struct _Anonymous_9
      {
        char* symbol;
        uint len;
      }

      _Anonymous_9 v_symbol;
      bson_decimal128_t v_decimal128;
    }

    _Anonymous_0 value;
  }

  alias bson_value_t = _bson_value_t;
  struct bson_iter_t {
    const(ubyte)* raw;
    uint len;
    uint off;
    uint type;
    uint key;
    uint d1;
    uint d2;
    uint d3;
    uint d4;
    uint next_off;
    uint err_off;
    bson_value_t value;
  }
  char * bson_iter_key(const bson_iter_t *iter);
  bson_value_t * bson_iter_value (bson_iter_t *iter);
  bool bson_iter_next(bson_iter_t *iter);
  bool bson_iter_init(bson_iter_t *iter, const bson_t *bson);
  bool bson_iter_init_find 
    (bson_iter_t *iter, const bson_t *bson, const char *key);
  bson_oid_t * bson_iter_oid (const bson_iter_t *iter);
  void bson_oid_init_from_string(bson_oid_t *oid, const char *str);
  bool bson_oid_is_valid (const char *str, size_t length);
  void bson_oid_to_string (const bson_oid_t *oid, char * str);
  bson_type_t bson_iter_type (const bson_iter_t *iter);
}
