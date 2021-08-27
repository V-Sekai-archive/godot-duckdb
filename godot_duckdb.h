#ifndef GDDUCKDB_H
#define GDDUCKDB_H

#include "core/config/engine.h"
#include "core/object/ref_counted.h"
#include "core/templates/local_vector.h"

#include "thirdparty/duckdb/duckdb.hpp"

class DuckDB;

class DuckDBQuery : public RefCounted {
  GDCLASS(DuckDBQuery, RefCounted);

  duckdb_prepared_statement stmt;
  String query;

protected:
  static void _bind_methods();

public:
  DuckDBQuery();
  ~DuckDBQuery();
  void init(duckdb_database *p_db, const String &p_query) {
    // db = p_db;
    // query = p_query;
    // stmt = nullptr;
  }
  bool is_ready() const;

  /// Returns the last error message.
  String get_last_error_message() const { return String(); }

  /// Executes the query.
  /// ```
  /// var query = db.create_query("SELECT * FROM table_name;")
  /// print(query.execute())
  /// # prints: [[0, 1], [1, 1], [2, 1]]
  /// ```
  ///
  /// You can also pass some arguments:
  /// ```
  /// var query = db.create_query("INSERT INTO table_name (column1, column2)
  /// VALUES (?, ?); ") print(query.execute([0,1])) # prints: []
  /// ```
  ///
  /// In case of error, a Variant() is returned and the error is logged.
  /// You can also use `get_last_error_message()` to retrieve the message.
  Variant execute(Array p_args = Array());

  /// Expects an array of arguments array.
  /// Executes N times the query, for N array.
  /// ```
  /// var query = db.create_query("INSERT INTO table_name (column1, column2)
  /// VALUES (?, ?); ") query.batch_execute([[0,1], [1,2], [2,3]])
  /// ```
  /// The above script insert 3 rows.
  ///
  /// Also works with a select:
  /// ```
  /// var query = db.create_query("SELECT * FROM table_name WHERE column1 = ?;")
  /// query.batch_execute([[0], [1], [2]])
  /// ```
  /// Returns: `[[0,1], [1,2], [2,3]]`
  Variant batch_execute(Array p_rows);

  /// Return the list of columns of this query.
  Array get_columns();

  void finalize();

private:
  bool prepare();
};

class DuckDB : public RefCounted {
  GDCLASS(DuckDB, RefCounted);

  friend class DuckDBQuery;

private:
  duckdb_database db;
  duckdb_connection con;
  duckdb_config config;

  LocalVector<WeakRef *> queries;
  bool open(String path);
  bool open_buffered(String name, PackedByteArray buffers, int64_t size);
  duckdb_prepared_statement prepare(const char *statement);
  Array fetch_rows(String query, Array args, int result_type = RESULT_BOTH);
  Dictionary parse_row(duckdb_prepared_statement stmt, int result_type);

public:
  static bool bind_args(duckdb_prepared_statement stmt, Array args);

protected:
  static void _bind_methods();

public:
  enum { RESULT_BOTH = 0, RESULT_NUM, RESULT_ASSOC };

  ~DuckDB();

  bool open_in_memory();
  void close();

  /// Compiles the query into bytecode and returns an handle to it for a faster
  /// execution.
  /// Note: you can create the query at any time, but you can execute it only
  /// when the DB is open.
  Ref<DuckDBQuery> create_query(String p_query);

  bool query(String statement);
  bool query_with_args(String statement, Array args);
  Array fetch_array(String statement);
  Array fetch_array_with_args(String statement, Array args);
  Array fetch_assoc(String statement);
  Array fetch_assoc_with_args(String statement, Array args);

  String get_last_error_message() const;
};
#endif
