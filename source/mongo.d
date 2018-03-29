module mongo;

import mongoaux.definitions;
import std.traits;

// TODO: Check date handling.
// TODO: Change exceptions. It should be possible to retry on failure.
// TODO: Add rest of unittests.
// TODO: Reply handling.
// TODO: Handle MongoDocCreated and MongoDocUpdated correctly.
// TODO: Remove the clients that used del from the pooler toDestroy to avoid
// consuming a lot of memory. Be careful not to remove it twice.


unittest {
  // TODO: Check BSONs on this test are deleted.

  // Use a pool to insert values, find them and delete them.
  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  foreach(i; 0..4) {
    auto client = pool.lock();
    scope(exit) client.unlock(); 
    // Not using the * would give compilation errors because indexes for
    // pointers are for memory locations.
    auto collection = (*(*client) ["newBase"])["newCollection"];

    if(collection.count) {
      collection.drop();
    }
    struct ExampleStruct {
      int intVal = 1;
      long longVal = 2L;
      double doubleVal = 3.0;
      int [] arrayVal = [1,2,3];
      int [][] multidimArray = [[1,2],[3,4]];
      string stringVal = "Text";
      int [string] aa;
    }
    // Insert empty BSON (it has only default values)
    // It'll have an _id in Mongo.
    auto toInsert = ExampleStruct();
    collection.insert(toInsert);
    assert(collection.count == 1);
    // Check that it has default value when converted back to ExampleStruct
    assert(collection.findOne.as!ExampleStruct == toInsert);
    collection.deleteOne();
    assert(collection.count == 0);

    // Add a non-default value.
    toInsert.longVal = 5;
    collection.insert(toInsert);
    assert(collection.count == 1);
    auto retValue = collection.findOne;
    assert(retValue.key!long(`longVal`) == 5L);
    // Check conversion again.
    assert(retValue.as!ExampleStruct == toInsert);
    // Be careful, this doesn't remove the value because deleteOne creates a
    // query with all the values in ExampleStruct. Insert didn't insert default
    // ones.
    collection.deleteOne(toInsert);
    assert(collection.count == 1);

    // Correct way: should have just the keys present in Mongo.
    // In this specific case, deleteOne without arguments would work too.
    struct CorrectQuery {
      long longVal = 5;
    }
    collection.deleteOne(CorrectQuery());
    assert(collection.count == 0);

    // Add array values.
    toInsert.arrayVal = [5,6,7,8];
    toInsert.multidimArray = [[7,8,9],[10,11,12]];
    toInsert.aa = ["one" : 1];
    collection.insert(toInsert);

    // Reuse retValue.
    retValue.unlock();
    retValue = collection.findOne;
    assert(retValue.key!(int[])(`arrayVal`) == [5,6,7,8]);
    assert(retValue.key!(int[][])(`multidimArray`) == [[7,8,9],[10,11,12]]);
    assert(retValue.key!(int[string])(`aa`)["one"] == 1);
    collection.deleteOne;

    // Empty arrays.
    toInsert.arrayVal = [];
    collection.insert(toInsert);

    // Reuse retValue.
    retValue.unlock();
    retValue = collection.findOne;
    scope(exit) retValue.unlock();
    assert(retValue.key!(int[])(`arrayVal`) == []);
    assert(retValue.as!ExampleStruct == toInsert);

    collection.deleteOne;

    // Update.
    
    toInsert.intVal = 4;
    collection.insert(toInsert);
    struct Query {
      struct I {
        @("$eq") int equalsTo = 4;
      }
      I intVal;
    }

    struct Update {
      struct I {
        int intVal = 2;
      }
      @("$set") I update;
    }
    auto upd = Update(); // Takes a ref so it can process UDAs.
    collection.update(Query(), upd);
    assert(collection.findOne!ExampleStruct.intVal == 2);
  }
}

unittest {
  // Manually built BSONs.
  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count){
    collection.drop();
  }

  auto toInsert = empty(); scope(exit) toInsert.unlock();
  toInsert.append(`intVal`, 4);
  toInsert.append(`longVal`, 9L);
  toInsert.append(`boolVal`, true);
  toInsert.append(`stringVal`, "Hello");
  toInsert.append(`doubleVal`, 5.0);
  auto subDocument = empty(); scope(exit) subDocument.unlock();
  subDocument.append(`subIntVal`, 7);
  toInsert.append(`subDoc`, subDocument);
  import std.datetime;
  // TODO: Change it to use just Unix timestamps.
  toInsert.append(`dateVal`, Clock.currTime (UTC ()));
  toInsert.append(`arrayVal`, [1, 2, 3, 4]);
  collection.insert(toInsert);

  auto foundVal = collection.findOne(toInsert);
  scope(exit) foundVal.unlock();
  foreach (key, value; foundVal.byKeyValue) {
    switch(key) {
      case `_id`:
        //writeln(value.as!bson_oid_t);
        break;
      case `intVal`:
        assert(value.as!int == 4);
        break;
      case `longVal`:
        assert(value.as!long == 9);
        break;
      case `boolVal`:
        assert(value.as!bool == true);
        break;
      case `stringVal`:
        assert(value.as!string == "Hello");
        break;
      case `doubleVal`:
        import std.math : approxEqual;
        assert(value.as!double.approxEqual(5.0));
        break;
      case `subDoc`:
        /+
        auto doc = value.as!BSON; scope(exit) doc.unlock();
        writeln(doc);
        +/
        break;
      case `dateVal`:
        //writeln(value.as!DateTime);
        break;
      case `arrayVal`:
        assert(value.as!(int[]) == [1,2,3,4]);
        break;
      default:
        assert(0, text(`TODO: `, key));
    }
  }
}

unittest {
  // Nested documents.
  struct Nested {
    struct Internal {
      int a = 0;
    }
    Internal internal;
  }
  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count){
    collection.drop();
  }
  auto toInsert = Nested(Nested.Internal(4));
  collection.insert(toInsert);
  auto found = collection.findOne;
  scope(exit) found.unlock();
  assert(found.as!Nested == Nested(Nested.Internal(4)));
}

unittest {
  // Use object ids
  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count) {
    collection.drop();
  }

  struct WithId {
    int a;
    bson_oid_t _id;
  }
  auto str = "aaaaaabbbbbbccccccdddddd";
  auto toInsert = WithId(5, str.toId);
  collection.insert(toInsert);
  auto retValue = collection.findOne;
  scope(exit) retValue.unlock();
  assert(retValue.as!WithId == toInsert);
  collection.deleteOne;
  
  struct WithStringId {
    string _id;
  }
  auto toInsert2 = WithStringId("123456789012345678901234");
  collection.insert(toInsert2);
  auto retValue2 = collection.findOne;
  scope(exit) retValue2.unlock();
  assert(retValue2.as!WithStringId == toInsert2);
}

unittest {
  // Without any BSON.
  struct ExampleStruct {
    int a = 0;
    long b = 1;
  }
  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count) {
    collection.drop();
  }

  auto toInsert = ExampleStruct(1,2);
  collection.insert(toInsert);
  assert(collection.findOne!ExampleStruct == toInsert);

  foreach(doc; collection.find!ExampleStruct) {
    assert(doc == toInsert);
  }
}

unittest {
  // Bulk operations.
  struct ExampleStruct {
    int a = 0;
    long b = 1;
    string _id;
  }

  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count) {
    collection.drop();
  }

  ExampleStruct [] dataRange;
  foreach(int i; 0..10) {
    string id;
    foreach(j; 0..24) {
      id ~= i.to!string;
    }
    dataRange ~= ExampleStruct(i, i, id);
  }

  collection.insert(dataRange);
  assert(collection.count == 10);
  uint i = 0;
  // Note: Might fail if Mongo sends them in other order, that wouldn't be
  // wrong.
  foreach(element; collection.find!ExampleStruct) {
    assert(element.a == i);
    assert(element.b == i);
    i++;
  }

  dataRange ~= ExampleStruct(10,10, "0123456789abcdef01234567");
  struct IdQuery {
    string _id;
  }
  struct SetValue {
    struct I {
      int a = 0;
      long b = 0;
    }
    @("$set") I val; // Equivalent to adding "$set" : {"a" : 0, "b" : 0} on a BSON
  }

  struct Upsert {
    bool upsert = true;
  }
  import std.algorithm : map;
  auto queryRange = dataRange.map!(a => IdQuery(a._id));
  auto updateRange = dataRange.map!(a => SetValue(SetValue.I(a.a + 1)));
  //writeln(collection.find!ExampleStruct);
  collection.update(queryRange, updateRange, Upsert());
  //writeln(collection.find!ExampleStruct);
  assert(collection.count == 11);
}

unittest {
  // MongoDocUpdated and MongoDocCreated.
  struct ExampleStruct {
    @MongoDocCreated long created;
    @MongoDocUpdated long updated;
  }

  auto pool = MongoPool("mongodb://localhost");
  scope(exit) pool.unlock();
  auto client = pool.lock();
  scope(exit) client.unlock(); 
  auto collection = (*(*client) ["newBase"])["newCollection"];

  if(collection.count) {
    collection.drop();
  }
  
  auto toInsert = ExampleStruct(0, 0);
  collection.insert(toInsert);
  assert(toInsert.created != 0 && toInsert.updated == 0);
}

import std.conv : to, text;
shared static this () {
  mongoc_init();
}
// Warning: Can be called before class destructors.
shared static ~this () {
  mongoc_cleanup();
}

import std.string : toStringz;
import std.array : Appender;
/// Wrapper over mongoc_client_pool_t that allows getting a client with
/// lock().
/// After usage this should be freed with unlock(). Note that this.unlock isn't 
/// the reverse operation of this.lock.
/// Clients should use their unlock().
struct MongoPool {
  mongoc_client_pool_t* pool  = null;
  mongoc_uri_t*         uri   = null;
  /// This is optional, allows cleaning when this.unlock() is called.
  /// But it keeps growing.
  debug Appender!(Client * []) toDestroy;
  this(string connectionString) {
    this.uri = mongoc_uri_new(connectionString.toStringz);

    // Warning: If this happens, the destructor will be called too.
    if(this.uri is null)
      throw new Exception("Invalid mongo uri");

    this.pool = mongoc_client_pool_new(this.uri);
    if(!this.pool)
      throw new Exception("Error creating pool");
  }

  /// Deallocates resources.
  auto unlock() {
    debug {
      foreach (ref client; toDestroy.data) {
        if (!client.deleted) {
          import std.stdio;
          "Warning: Didn't manually delete a client".writeln;
        }
        client.unlock();
      } 
      toDestroy.clear();
    }

    if (this.uri)
      mongoc_uri_destroy(this.uri);
    if (this.pool)
      mongoc_client_pool_destroy(this.pool);
    this.uri = null;
    this.pool = null;
  }

  Client * lock() {
    auto mongocClient = mongoc_client_pool_pop(this.pool);
    if(!mongocClient) {
      throw new Exception(`Error creating client from pool`);
    }
    auto toReturn = new Client(mongocClient, this.pool);
    debug {
      toDestroy ~= toReturn;
    }
    return toReturn;
  }
}

struct Client {
  mongoc_client_t * client;
  Appender!(Database * []) toDestroy;
  // Null if this wasn't created from a pooler.
  mongoc_client_pool_t * parentPool = null; 
  bool deleted = false; // Prevents deleting multiple times.
  @disable this();
  this(string uri) {
    this.client = mongoc_client_new(uri.toStringz);
    if(!client) {
      throw new Exception(`Error creating client.`);
    }
  }
  this(mongoc_client_t * client, mongoc_client_pool_t * pool) {
    assert(client && pool);
    this.parentPool = pool;
    this.client     = client;
  }
  auto opIndex(string baseName) {
    assert(client);
    auto toReturn = new Database(
      mongoc_client_get_database(client, baseName.toStringz)
    );
    toDestroy ~= toReturn;
    return toReturn;
  }
  /// Deallocates resources.
  /// If this has a parentPool then it doesn't destroy this.client
  auto unlock() {
    if(deleted) return;
    assert(client);
    foreach(ref database; toDestroy.data) {
      database.unlock();
    }
    toDestroy.clear();
    if(this.parentPool) {
      mongoc_client_pool_push(this.parentPool, this.client);
      parentPool = null;
    } else {
      mongoc_client_destroy(client);
    }
    client = null; 
    deleted = true;
  }
}
struct Database {
  @disable this();
  mongoc_database_t * database;
  Appender!(Collection []) toDestroy;
  this(mongoc_database_t * database) {
    this.database = database;
  }
  auto opIndex(string collectionName) {
    assert(database);
    auto toReturn = Collection(
      mongoc_database_get_collection(
        database, collectionName.toStringz
      )
    );
    toDestroy ~= toReturn;
    return toReturn;
  }
  auto unlock() {
    assert(database);
    foreach(ref collection; toDestroy.data) {
      collection.unlock();
    }
    toDestroy.clear;
    mongoc_database_destroy(database);
    database = null;
  }
}

// Used for automatic setting of fields when doing operations.
// Note: As of now it updates the MongoDocUpdated on findAndModify and
// update and MongoDocCreated on insert.
// Maybe one should use https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/ and $set with $currentDate
enum MongoDocCreated;
enum MongoDocUpdated;

struct Collection {
  @disable this();
  mongoc_collection_t * collection;
  this(mongoc_collection_t * collection) {
    this.collection = collection;
  }
  auto unlock() {
    assert (collection);
    mongoc_collection_destroy(collection);
    collection = null;
  }

  auto drop() {
    assert(collection);
    bson_error_t error;
    if(!mongoc_collection_drop(collection, &error)) {
      throw new Exception(text(`Error in drop: `, error));
    }
  }

  long count(T1 = BSON)(
    T1 query = empty!T1()
    , mongoc_query_flags_t flags = mongoc_query_flags_t.NONE
    , long skip = 0
    , long limit = 0
    , const mongoc_read_prefs_t * readPrefs = null
  ) {
    assert(collection);
    bson_error_t error;
    long retValue = mongoc_collection_count(
      collection
      , flags
      , ScopedBSON(query, false, false).data
      , skip
      , limit
      , readPrefs
      , &error
    );
    if (retValue == -1) {
      throw new Exception(text(`Error in Collection.count: `, error.message));
    } else {
      return retValue;
    }
  }

  /// Convenience method for calling mongoc's insert with the upsert flag.
  auto findAndModify(T1 = BSON, T2 = BSON, T3 = BSON, T4 = BSON)(
    ref T1 query
    , T2 update
    , T3 sort = empty!T3()
    , T4 fields = empty!T4()
    , bool remove = false
    , bool upsert = false
    , bool _new = true
    , BSON reply = empty()
  ) {
    // Note: this assumes every document is updated, not created.
    processMongoDocUDAs!MongoDocUpdated(update);
    bson_error_t error;
    if(!mongoc_collection_find_and_modify(
      this.collection
      // Don't ignore defaults for all the parameters.
      , ScopedBSON(query, false, false).data
      , ScopedBSON(sort, false).data
      , ScopedBSON(update, false).data
      , ScopedBSON(fields, false).data
      , remove
      , upsert
      , _new
      , reply.data
      , &error
    )) {
      throw new Exception(text(`Error on findAndModify: `, error));
    }
  }

  /// Takes either one query and document or
  /// a range of queries and another of documents..
  auto update(T1 = BSON, T2 = BSON, T3 = BSON)(
    T1 queries
    , ref T2 documents // ref so that MongoDocUpdated can be set.
    , T3 options = empty!T3()
  ){
    static if(!isInputRange!T1) {
      // Single document update.
      alias query = queries;
      alias document = documents;

      bson_error_t error;
      processMongoDocUDAs!MongoDocUpdated(document);
      if(!mongoc_collection_update_one(
          this.collection
          , ScopedBSON(query, false, false).data
          , ScopedBSON(document, false, false).data
          , ScopedBSON(options, false).data
          , ScopedBSON(empty(), false).data //Reply
          , &error
      )) {
        throw new Exception(text(`Error inserting: `, error.message));
      }
    } else {
      // Queries and documents are ranges => Bulk update.
      static assert(
        isInputRange!T2
        , `Both queries and documents should be ranges or single data`
      );
      
      assert(this.collection);
      mongoc_bulk_operation_t* bulk;
      bulk = mongoc_collection_create_bulk_operation_with_opts(
        this.collection
        , null // opts
      );
      if(!bulk) throw new Exception(`Error creating bulk operation`);
      scope(exit) mongoc_bulk_operation_destroy(bulk);
      bson_error_t error;

      // Note: Name clash with empty.
      import std.range : empty, front, popFront; 
      foreach(ref document; documents) {
        // Assumes just setting updated is enough.
        processMongoDocUDAs!MongoDocUpdated(document);
        assert(!queries.empty, `More documents than queries`);
        if(!mongoc_bulk_operation_update_one_with_opts(
          bulk
          , ScopedBSON(queries.front, false, false).data
          , ScopedBSON(document, false, false).data
          , ScopedBSON(options, false).data
          , &error
        )) {
          throw new Exception(text(
            `Error adding document to bulk update `
            , error.message)
          );
        }
        queries.popFront();
      }
      assert(queries.empty, `More queries than documents`);
      if(!mongoc_bulk_operation_execute(
        bulk
        , null /*reply that must be freed*/
        , &error
      )) {
        throw new Exception(text(`Error executing bulk operation`, error));
      }
    }
  }

  /// If toInsert is a range, then bulk inserts are used.
  auto insert(T1, T2 = BSON)(
    ref T1 toInsert
    , T2 options = empty!T2()
    , BSON reply = empty()
  ) {
    assert(this.collection);
    static if(isForwardRange!T1) {
      // Ranges are inserted in bulk.
      import std.algorithm : count;
      auto amount = count(toInsert);
      assert(amount > 0);
      bson_t* [] toSend = [];
      import std.range.primitives : hasLength;
      static if (hasLength!T1) {
        toSend.reserve(toInsert.length);
      }
      // Possible optimization: Create array on the stack
      // instead of allocating BSONs.
      foreach(ref element; toInsert) {
        processMongoDocUDAs!MongoDocCreated(element);
        auto toAppend = element.toBSON;
        toSend ~= toAppend.data;
      }
      this.insertMany(toSend, options, reply);
      foreach(toDestroy; toSend) {
        bson_destroy(toDestroy);
      }
      /+
      foreach(i; 0..toSend.length) {
        bson_destroy(&toSend[i]);
      }+/
    } else {
      // Single document insert.
      bson_error_t error;
      processMongoDocUDAs!MongoDocCreated(toInsert);
      if(!mongoc_collection_insert_one(
        this.collection
        , ScopedBSON(toInsert, true, false).data
        , ScopedBSON(options, false).data
        , reply.data
        , &error
      )) {
        throw new Exception(text(`Error inserting: `, error));
      }
    }
  }


  /// Useful if you already have an array of bson_t, otherwise it's better
  /// to use insert with a range as it builds that array.
  auto insertMany(T = BSON)(
    bson_t * [] toInsert
    , T options = empty!T()
    , BSON reply = empty()
  ) {
    bson_error_t error;
    assert(this.collection);
    if(!mongoc_collection_insert_many(
      this.collection
      , toInsert.ptr
      , toInsert.length
      , ScopedBSON(options, false).data
      , reply.data
      , &error
    )) {
      throw new Exception(text(`Error bulk-inserting: `, error));
    }
  }

  /// Convenience function for getting just one element from a find.
  /// Must be deleted with unlock().
  ReturnType findOne(ReturnType = BSON, T1 = BSON, T2 = BSON)(
    T1 filter = empty!T1()
    , T2 options = empty!T2()
    , const mongoc_read_prefs_t * readPrefs = null
  ) {
    auto cursor = this.find(filter, options, readPrefs);
    scope(exit) cursor.unlock();
    if(cursor.empty) {
      throw new Exception(`Didn't find on findOne: ` ~ filter.toString);
    }
    auto data = bson_copy(cursor.front.data);
    if(!data) {
      throw new Exception(`Couldn't allocate BSON`);
    }
    static if(is(ReturnType == BSON)) {
      return BSON(data);
    } else {
      return(ScopedBSON(BSON(data)).as!ReturnType);
    }
  }

  auto find(ReturnType = BSON, T1 = BSON, T2 = BSON)(
    T1 filter = empty!T1()
    , T2 options = empty!T2()
    , const mongoc_read_prefs_t * readPrefs = null
  ) {
    assert(this.collection);
    return Cursor!ReturnType(mongoc_collection_find_with_opts(
      this.collection
      , ScopedBSON(filter, false, false).data
      , ScopedBSON(options, false).data
      , readPrefs
    ));
  }

  auto deleteOne(T1 = BSON,T2 = BSON)(
    T1 selector = empty!T1()
    , T2 options = empty!T2()
    , BSON reply = empty()
  ) {
    assert(collection);
    bson_error_t error;
    if(!mongoc_collection_delete_one (
      this.collection
      , ScopedBSON(selector, false, false).data
      , ScopedBSON(options).data
      , reply.data
      , &error
    )) {
      throw new Exception(text(`Couldn't delete `, error));
    }
  }

  /// Sets the fields with UDA of struct T with the current unix time.
  /// Does nothing if T is a BSON.
  void processMongoDocUDAs(alias UDA, T)(ref T val) {
    static if(__traits(isPOD, T) && isAggregateType!T) {
      alias created = getSymbolsByUDA!(T, UDA);
      static assert(
        created.length < 2
        , `Are you sure you want several ` ~ UDA.stringof 
          ~ ` symbols in ` ~ S.stringof ~ `?`
      );
      import std.datetime;
      static if(created.length) {
        static assert(is(typeof(created [0]) == long));
        // Eg. val.insertedTime = Clock.currTime(UTC()).toUnixTime;
        mixin(
          `val.` ~ __traits(identifier, created [0]) 
          ~ ` = Clock.currTime(UTC()).toUnixTime;`
        );
      }
    } else {
      static assert(
        is(T == BSON)
        , `Cannot process mongo UDAs for type ` ~ T.stringof
      );
    }
  }
}

enum MongoKeep;

/// Fills a BSON with the members from a struct.
/// if ignoreDefaults is true, then values with the default value aren't added.
/// this behavior is useful to toggle it on insertions but off for option
/// or query parameters.
auto ref fillBSON(Type)(Type instance, ref BSON toFill, bool ignoreDefaults = true) {
  static foreach(field; FieldNameTuple!Type) { {
    auto instanceField = __traits(getMember, instance, field);
    // Save only the fields with non default values or the ones that
    // have the @MongoKeep UDA.
    if (
        (!ignoreDefaults)
        || instanceField != __traits(getMember, Type.init, field)
        || hasUDA!(mixin(`Type.` ~ field), MongoKeep)
       ) {
      // Fields with string UDAs should append documents with the document key
      // as a string, for example @("$set") int a = 3;
      // would add a $set : { "a" : 3 }
      toFill.append(strUDA!(Type, field), instanceField, ignoreDefaults);
    }
  } }
}

// Search for a string UDA and return it's value.
string strUDA (alias Type, string fieldName)() {
  string toReturn = fieldName;
  mixin(`alias FieldType = Type.` ~ fieldName ~ `;`);
  static foreach(uda; __traits(getAttributes, mixin(`Type.` ~ fieldName))) {
    static if(is(typeof(uda) == string)) {
      /+
      static assert(
        __traits(isPOD, typeof(mixin(`Type.` ~ fieldName)))
        && isAggregateType!(typeof(mixin(`Type.` ~ fieldName)))
        , `Fields with UDA strings should be POD structs: `
        ~ Type.stringof ~ `.` ~ fieldName
      );
      +/
      toReturn = uda;
    }
  }
  return toReturn;
}

/// ignoreDefaults does nothing, is just for compatibility with the other toBSON.
auto ref toBSON(BSON ob, bool ignoreDefaults = true) {return ob;}
/// Converts a POD struct to a BSON object.
auto toBSON(Type)(Type instance, bool ignoreDefaults = true) {
  static assert(!is(Type == BSON));
  static assert(
    __traits(isPOD, Type) && isAggregateType!Type
    , `bson(instance) is only implemented for POD structs`
  );

  auto toReturn = empty!(BSON, true)();
  instance.fillBSON(toReturn, ignoreDefaults);
  return toReturn;
}

/// A BSON that is destroyed if created from a struct on its destructor and
/// just a wrapper over an existing BSON otherwise.
struct ScopedBSON {
  BSON bson;
  bool deleteOnDestructor = false;
  @disable this();
  // unused is for consistency with the other constructor.
  this(ref BSON other, bool unused = true, bool allowNull = true){
    if (!other.data && !allowNull){
      this.bson = empty!(BSON, true);
      deleteOnDestructor = true;
    } else {
      this.bson = other;
    }
  }
  this(S)(S other, bool ignoreDefaults = true, bool unused = true){
    this.bson = other.toBSON(ignoreDefaults);
    deleteOnDestructor = true;
  }
  ~this(){
    if(deleteOnDestructor){
      bson.unlock();
    }
  }
  alias bson this;

}

/// Used for iterating mongoc_cursor_ts as input ranges.
struct Cursor(ElementType = BSON){
  mongoc_cursor_t * cursor;
  @disable this();
  this(mongoc_cursor_t * cursor) {
    this._front = .empty();
    this.cursor = cursor;
    popFront ();
  }
  // Note: _front.unlock () shouldn't be called, mongoc sets it automatically.
  BSON _front;
  bool empty = false;
  auto popFront () {
    if(!mongoc_cursor_next(cursor, &_front.data)) {
      bson_error_t error;
      if (mongoc_cursor_error(this.cursor, &error)) {
        throw new Exception(`Cursor error`);
      } else {
        // Empty cursor.
        this.unlock();
      }
    }
  }

  ElementType front () {
    static if(is(ElementType == BSON)) {
      return _front;
    } else {
      return _front.as!ElementType;
    }
  }

  /// Called automatically on exhaustion, useful if not all the elements are
  /// desired.
  void unlock() {
    mongoc_cursor_destroy(this.cursor);
    this.cursor = null;
    empty = true;
  }
}
import std.range : isInputRange, isForwardRange;
static assert(isInputRange!(Cursor!BSON));

/// Creates a BSON from a JSON string.
/// The BSON needs to be destroyed manually with unlock()
BSON fromJSON(string json) {
  bson_error_t error;
  bson_t * data;
  data = bson_new_from_json(
    cast (const ubyte*) json
    , json.length.to!ssize_t
    , &error
  );
  if (!data) {
    throw new Exception(text(`Error converting JSON to BSON: `, error));
  }
  auto toReturn = BSON(data);
  return toReturn;
}

auto empty(Type = BSON, bool initialize = false)() {
  static if(is(Type == BSON)) {
    static if(!initialize) {
      return BSON(null);
    } else {
      auto toReturn = BSON(null);
      toReturn.initialize();
      return toReturn;
    }
  } else {
    return Type.init;
  }
}

struct BSON {
  @disable this();
  // Could use a statically allocated bson_t but it gave problems with automatic
  // destruction on scope exits.
  bson_t * data = null; 
  this(bson_t * data) {
    this.data = data;
  }
  /// empty is used for creating Bsons without fields. Structs cannot have a
  /// constructor without parameters, so it's a workaround.
  this(Args...)(Args args) {
    static assert (
      args.length % 2 == 0
      , `BSON constructor requires an even number of parameters`
    );
    this.initialize();
    assert (0, `TODO`);
  }
  /// Used to new this.data.
  auto initialize() {
    this.data = bson_new ();
    if (!data) {
      throw new Exception(`Error creating Bson`);
    }
  }
  auto unlock() {
    if(this.data) {
      bson_destroy(data);
      this.data = null;
    }
  }
  string toString() const {
    if(!data) return `null`;
    auto str = bson_as_canonical_extended_json(data, null);
    if (!str) {
      throw new Exception(`Error creating string from BSON`);
    }
    scope(exit) bson_free(str);
    return str.to!string;
  }

  /// name must be ASCII
  void append(T)(string name, T val, bool ignoreSubDocDefaults = false) {
    if(!data) this.initialize();
    import std.ascii : isASCII;
    import std.algorithm : all;
    debug assert(name.all!isASCII);
    bool success = true;
    import std.datetime : SysTime;
    import std.range : isInputRange;
    // Abstracts the common pattern of append operations.
    bool appendOp(alias fun, Args...)(Args args){
      assert(data);
      return fun(
        data
        , name.toStringz
        , name.length.to!int
        , args
      );
    }
    static if(is(T == string)) {
      if(name == `_id`) { // Automatic conversion to bson_oid_t
        auto toAppend = val.toId;
        success = appendOp!bson_append_oid(
            &toAppend
        );
      } else {
        success = appendOp!bson_append_utf8(
          val.toStringz
          , val.length.to!int
        );
      }
    } else static if(is(T == SysTime)) {
      // 4 parameters but must cast to Unix time.
      success = appendOp!bson_append_date_time(
          val.toUnixTime
      );
    } else static if(is(T == BSON)) {
      success = appendOp!bson_append_document(val.data);
    } else static if(isInputRange!T) {
      // Append as array:
      BSON arr = empty!(BSON, true)(); 
      // TODO: Check if needed: //scope(exit) arr.unlock();

      success = appendOp!bson_append_array_begin(arr.data);
      foreach(i, element; val) {
        arr.append(i.to!string, element);
      }
      success &= bson_append_array_end(
        data
        , arr.data
      );
    } else static if(isAssociativeArray!T && is(T Key: Key[Value], Value)){
      BSON arr = empty!(BSON, true)();
      success = appendOp!bson_append_array_begin(arr.data);
      foreach(key, value; val) {
        arr.append(key, value);
      }
      success &= bson_append_array_end(
        data
        , arr.data
      );

    } else static if(is(T == bson_oid_t)){
      success = appendOp!bson_append_oid(&val);
    } else static if(isAggregateType!T && !is(T == bson_oid_t)) {
      BSON toAppend = val.toBSON(ignoreSubDocDefaults);
      success = appendOp!bson_append_document(toAppend.data);
      // TODO: Check if toAppend.unlock is needed.
    } else {
      // 4 parameter appends.
      static if(is(T == int)) {
        alias fun = bson_append_int32;
      } else static if(is(T == long)) {
        alias fun = bson_append_int64;
      } else static if(is(T == bool)) {
        alias fun = bson_append_bool;
      } else static if(is(T == double)) {
        alias fun = bson_append_double;
      } else {
        static assert(0, `Unrecognised type for appending to BSON `~ T.stringof);
      }
      success = appendOp!fun(val);
    }
    if (!success) {
      throw new Exception(`Error appending`);
    }
  }

  auto byKeyValue() {
    return BSONIter(data);
  }
  
  // Note, this does seem to be O(n).
  auto key(Type)(string key) {
    assert(data);
    bson_iter_t iterator;
    if(!bson_iter_init_find(&iterator, data, key.toStringz)) {
      throw new Exception(`Problem looking for key ` ~ key ~ ` in BSON`);
    }
    return bson_iter_value(&iterator).as!Type;
  
  }
  T as(T)() {
    static assert(
      __traits(isPOD, T) && isAggregateType!T
      , `BSON.as is made for POD structs. Check the code before using it with `
        ~ T.stringof
    );
    alias typeFields = FieldNameTuple!T;
    T toReturn;
    foreach(key, value; this.byKeyValue){
      outerSwitch: switch(key) {
        static foreach(field; typeFields) {
          case field:
            alias FieldType = typeof(mixin(`T.` ~ field));
            FieldType toAssign;
            static if (field == `_id` && is(FieldType == string)) {
              toAssign = value.as!bson_oid_t.fromId;
            } else {
              toAssign = value.as!FieldType;
            }
            enum fieldToAssign = `toReturn.` ~ field;
            mixin(fieldToAssign ~ ` = toAssign;`);
            break outerSwitch;
        }
        default:
          // _id is the only field that is allowed to be on the BSON
          // and not on the struct, if bo has some other field that the
          // struct doesn't, an exception is thrown.
          if(key != `_id`){
            throw new Exception (`Found member of BSON that is not in `
              ~ T.stringof ~ ` : ` ~ key);
          }
      }
    }
    return toReturn;
  }
  //alias data this;
}

/// Used just for 'as' function.
struct DateTime {
  long value;
  alias value this;
}
/// Converts a bson_value_t to another type.
/// If BSON is used as a type, it assumes it's a document.
/// Make sure to unlock() that BSON.
auto as(type)(bson_value_t * val) {
  assert(val);
  auto vval = val.value;
  auto vtype = val.value_type;
  static if(is(type == int)){
    assert(vtype == bson_type_t.BSON_TYPE_INT32);
    return vval.v_int32;
  } else static if(is(type == long)) {
    assert(vtype == bson_type_t.BSON_TYPE_INT64);
    return vval.v_int64;
  } else static if(is(type == bool)) {
    assert(vtype == bson_type_t.BSON_TYPE_BOOL);
    return vval.v_bool;
  } else static if(is(type == string)) {
    assert(vtype == bson_type_t.BSON_TYPE_UTF8);
    auto toReturn = vval.v_utf8.str[0..vval.v_utf8.len];
    return toReturn.to!string;
  } else static if(is(type == double)) {
    assert(vtype == bson_type_t.BSON_TYPE_DOUBLE);
    return vval.v_double;
  } else static if(is(type == bson_oid_t)) {
    assert(vtype == bson_type_t.BSON_TYPE_OID);
    return vval.v_oid;
  } else static if(is(type == BSON)) {
    assert(vtype == bson_type_t.BSON_TYPE_DOCUMENT);
    auto toReturn = bson_new_from_data(vval.v_doc.data, vval.v_doc.data_len);
    return BSON(toReturn);
  } else static if(is(type == DateTime)) {
    assert(vtype == bson_type_t.BSON_TYPE_DATE_TIME);
    return DateTime(vval.v_datetime);
  } else static if(isArray!type) {
    assert(vtype == bson_type_t.BSON_TYPE_ARRAY);
    auto asDoc = bson_new_from_data(vval.v_doc.data, vval.v_doc.data_len);
    auto asBSON = BSON(asDoc); scope(exit) asBSON.unlock();
    import std.array : Appender;
    import std.range : ElementType;
    Appender!type toReturn;
    foreach(element; BSONIter(asBSON.data)) {
      import std.algorithm : map;
      toReturn ~= element.value.as!(ElementType!type);
    }
    return toReturn.data;
  } else static if(isAssociativeArray!type && is(type Value: Value[Key], Key)) {
    assert(vtype == bson_type_t.BSON_TYPE_ARRAY);
    auto asDoc = bson_new_from_data(vval.v_doc.data, vval.v_doc.data_len);
    auto asBSON = BSON(asDoc); scope(exit) asBSON.unlock();
    type toReturn;
    foreach(key, value; BSONIter(asBSON.data)) {
      toReturn[key] = value.as!Value;
    }
    return toReturn;
  } else static if(isAggregateType!type) {
    assert(vtype == bson_type_t.BSON_TYPE_DOCUMENT);
    auto docBSON = val.as!BSON;
    scope(exit) docBSON.unlock();
    return docBSON.as!type;
  } else static assert (0, `TODO: as!` ~ type.stringof);
}

/// Empty if constructed with null.
struct BSONIter {
  bson_iter_t iter;
  @disable this();
  bool empty = false;
  auto front() {
    assert(!empty);
    import std.typecons : Tuple;
    return Tuple!(string, `key`, bson_value_t *, `value`)(
      bson_iter_key(&iter).to!string
      , bson_iter_value(&iter)
    );
  }
  auto popFront () {
    assert(!empty);
    if(!bson_iter_next(&iter)) {
      this.empty = true;
    }
  }
  this(bson_t * toIterate) {
    if(!toIterate) {
      empty = true;
    } else {
      if(bson_iter_init (&iter, toIterate)) {
        popFront();
      } else {
        throw new Exception (`Failed to create BSON iterator`);
      }
    }
  }
}

/// Converts a string to an object id.
bson_oid_t toId(string representation) {
  bson_oid_t toReturn;
  auto cstr = representation.toStringz;
  if(!bson_oid_is_valid(cstr, representation.length)) {
    throw new Exception(`Invalid string for _id conversion`);
  }
  bson_oid_init_from_string(&toReturn, cstr); 
  return toReturn;
}

string fromId(bson_oid_t id) {
  char[25] toInsertTo; //Includes \0
  bson_oid_to_string(&id, toInsertTo.ptr);
  return toInsertTo[0..24].to!string;
}
