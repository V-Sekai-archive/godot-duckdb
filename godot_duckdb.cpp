#include "godot_duckdb.h"

#include "core/core_bind.h"
#include "core/os/os.h"
#include "editor/project_settings_editor.h"

#include "thirdparty/duckdb/duckdb.hpp"

DuckDBQuery::DuckDBQuery() {
}

DuckDBQuery::~DuckDBQuery() {
	finalize();
}

bool DuckDBQuery::is_ready() const {
	return stmt != nullptr;
}

Variant DuckDBQuery::execute(const Array p_args) {
	// if (is_ready() == false) {
	// 	ERR_FAIL_COND_V(prepare() == false, Variant());
	// }

	// // At this point stmt can't be null.
	// CRASH_COND(stmt == nullptr);

	// // Error occurred during argument binding
	// if (!DuckDB::bind_args(stmt, p_args)) {
	// 	ERR_FAIL_V_MSG(Variant(), "Error during arguments set: " + get_last_error_message());
	// }

	// Execute the query.
	Array result;
	// while (true) {
	// 	const int res = sqlite3_step(stmt);
	// 	if (res == SQLITE_ROW) {
	// 		// Collect the result.
	// 		result.append(fast_parse_row(stmt));
	// 	} else if (res == SQLITE_DONE) {
	// 		// Nothing more to do.
	// 		break;
	// 	} else {
	// 		// Error
	// 		ERR_BREAK_MSG(true, "There was an error during an SQL execution: " + get_last_error_message());
	// 	}
	// }

	// if (SQLITE_OK != sqlite3_reset(stmt)) {
	// 	finalize();
	// 	ERR_FAIL_V_MSG(result, "Was not possible to reset the query: " + get_last_error_message());
	// }

	return result;
}

Variant DuckDBQuery::batch_execute(Array p_rows) {
	Array res;
	for (int i = 0; i < p_rows.size(); i += 1) {
		ERR_FAIL_COND_V_MSG(p_rows[i].get_type() != Variant::ARRAY, Variant(), "An Array of Array is exepected.");
		Variant r = execute(p_rows[i]);
		if (unlikely(r.get_type() == Variant::NIL)) {
			// An error occurred, the error is already logged.
			return Variant();
		}
		res.push_back(r);
	}
	return res;
}

Array DuckDBQuery::get_columns() {
	// if (is_ready() == false) {
	// 	ERR_FAIL_COND_V(prepare() == false, Array());
	// }

	// // At this point stmt can't be null.
	// CRASH_COND(stmt == nullptr);

	Array res;
	// const int col_count = sqlite3_column_count(stmt);
	// res.resize(col_count);

	// // Fetch all column
	// for (int i = 0; i < col_count; i++) {
	// 	// Key name
	// 	const char *col_name = sqlite3_column_name(stmt, i);
	// 	res[i] = String(col_name);
	// }

	return res;
}

bool DuckDBQuery::prepare() {

	// ERR_FAIL_COND_V(stmt != nullptr, false);
	// ERR_FAIL_COND_V(db == nullptr, false);
	// ERR_FAIL_COND_V(query == "", false);

	// duckdb_state state;
	// state = duckdb_prepare(con, query.utf8().ptr(), &stmt);
	// ERR_FAIL_COND_V_MSG(state == DuckDBError, false, "Cannot prepare SQL statement.");
	return true;
}

void DuckDBQuery::finalize() {
	// if (stmt) {
	// 	sqlite3_finalize(stmt);
	// 	stmt = nullptr;
	// }
}

void DuckDBQuery::_bind_methods() {
	ClassDB::bind_method(D_METHOD("get_last_error_message"), &DuckDBQuery::get_last_error_message);
	ClassDB::bind_method(D_METHOD("execute", "arguments"), &DuckDBQuery::execute, DEFVAL(Array()));
	ClassDB::bind_method(D_METHOD("batch_execute", "rows"), &DuckDBQuery::batch_execute);
	ClassDB::bind_method(D_METHOD("get_columns"), &DuckDBQuery::get_columns);
}

/*
	Open a database file.
	If this is running outside of the editor, databases under res:// are assumed to be packed.
	@param path The database resource path.
	@return status
*/
bool DuckDB::open(String path) {
	
	// if (!path.strip_edges().length())
	// 	return false;

	// if (!Engine::get_singleton()->is_editor_hint() && path.begins_with("res://")) {
	// 	Ref<core_bind::File> dbfile;
	// 	dbfile.instantiate();
	// 	if (dbfile->open(path, core_bind::File::READ) != Error::OK) {
	// 		print_error("Cannot open packed database!");
	// 		return false;
	// 	}
	// 	int64_t size = dbfile->get_length();
	// 	PackedByteArray buffer = dbfile->get_buffer(size);
	// 	return open_buffered(path, buffer, size);
	// }

	// String real_path = ProjectSettings::get_singleton()->globalize_path(path.strip_edges());

	// int result = sqlite3_open(real_path.utf8().get_data(), &db);

	// if (result != SQLITE_OK) {
	// 	print_error("Cannot open database!");
	// 	return false;
	// }

	return true;
}

bool DuckDB::open_in_memory() {
	// duckdb_state state = duckdb_open(NULL, &db);
	// ERR_FAIL_COND_V_MSG(state == DuckDBError, false, "Cannot open database in memory");
	return true;
}

/*
  Open the database and initialize memory buffer.
  @param name Name of the database.
  @param buffers The database buffer.
  @param size Size of the database; 
  @return status
*/
bool DuckDB::open_buffered(String name, PackedByteArray buffers, int64_t size) {
	if (!name.strip_edges().length()) {
		return false;
	}

	if (!buffers.size() || !size) {
		return false;
	}

	if (duckdb_create_config(&config) == DuckDBError) {
		return false;
	}
	// set some configuration options
	duckdb_set_config(config, "access_mode", "READ_ONLY");

	if (duckdb_open(name.utf8().get_data(), &db) == DuckDBError) {
		return false;
	}
	
	return true;
}

void DuckDB::close() {
	// Finalize all queries before close the DB.
	// Reverse order because I need to remove the not available queries.
	for (uint32_t i = queries.size(); i > 0; i -= 1) {
		DuckDBQuery *query = Object::cast_to<DuckDBQuery>(queries[i - 1]->get_ref());
		if (query != nullptr) {
			query->finalize();
		} else {
			memdelete(queries[i - 1]);
			queries.remove(i - 1);
		}
	}

	duckdb_disconnect(&con);
	duckdb_close(&db);
}

duckdb_prepared_statement DuckDB::prepare(const char *query) {
	duckdb_prepared_statement stmt;
	// duckdb_state state = duckdb_prepare(conn, query, -1, &stmt, nullptr);
	// ERR_FAIL_COND_V_MSG(state == DuckDBError, nullptr, "SQL Error");
	return stmt;
}

bool DuckDB::bind_args(duckdb_prepared_statement stmt, Array args) {
	// // Check parameter count
	// int param_count = sqlite3_bind_parameter_count(stmt);
	// if (param_count != args.size()) {
	// 	print_error("DuckDBQuery failed; expected " + itos(param_count) + " arguments, got " + itos(args.size()));
	// 	return false;
	// }

	// /**
	//  * DuckDB data types:
	//  * - NULL
	//  * - INTEGER (signed, max 8 bytes)
	//  * - REAL (stored as a double-precision float)
	//  * - TEXT (stored in database encoding of UTF-8, UTF-16BE or UTF-16LE)
	//  * - BLOB (1:1 storage)
	//  */

	// for (int i = 0; i < param_count; i++) {
	// 	int retcode;
	// 	switch (args[i].get_type()) {
	// 		case Variant::Type::NIL:
	// 			retcode = sqlite3_bind_null(stmt, i + 1);
	// 			break;
	// 		case Variant::Type::BOOL:
	// 		case Variant::Type::INT:
	// 			retcode = sqlite3_bind_int(stmt, i + 1, (int)args[i]);
	// 			break;
	// 		case Variant::Type::FLOAT:
	// 			retcode = sqlite3_bind_double(stmt, i + 1, (double)args[i]);
	// 			break;
	// 		case Variant::Type::STRING:
	// 			retcode = sqlite3_bind_text(stmt, i + 1, String(args[i]).utf8().get_data(), -1, SQLITE_TRANSIENT);
	// 			break;
	// 		case Variant::Type::PACKED_BYTE_ARRAY:
	// 			retcode = sqlite3_bind_blob(stmt, i + 1, PackedByteArray(args[i]).ptr(), PackedByteArray(args[i]).size(), SQLITE_TRANSIENT);
	// 			break;
	// 		default:
	// 			print_error("DuckDB was passed unhandled Variant with TYPE_* enum " + itos(args[i].get_type()) + ". Please serialize your object into a String or a PoolByteArray.\n");
	// 			return false;
	// 	}

	// 	if (retcode != SQLITE_OK) {
	// 		print_error("DuckDBQuery failed, an error occured while binding argument" + itos(i + 1) + " of " + itos(args.size()) + " (DuckDB errcode " + itos(retcode) + ")");
	// 		return false;
	// 	}
	// }

	return true;
}

bool DuckDB::query_with_args(String query, Array args) {
	// sqlite3_stmt *stmt = prepare(query.utf8().get_data());

	// // Failed to prepare the query
	// ERR_FAIL_COND_V_MSG(stmt == nullptr, false, "DuckDBQuery preparation error: " + get_last_error_message());

	// // Error occurred during argument binding
	// if (!bind_args(stmt, args)) {
	// 	sqlite3_finalize(stmt);
	// 	ERR_FAIL_V_MSG(false, "Error during arguments bind: " + get_last_error_message());
	// }

	// // Evaluate the sql query
	// sqlite3_step(stmt);
	// sqlite3_finalize(stmt);

	return true;
}

Ref<DuckDBQuery> DuckDB::create_query(String p_query) {
	Ref<DuckDBQuery> query;
	query.instantiate();
	// query->init(this, p_query);

	// WeakRef *wr = memnew(WeakRef);
	// wr->set_obj(query.ptr());
	// queries.push_back(wr);

	return query;
}

bool DuckDB::query(String query) {
	return this->query_with_args(query, Array());
}

Array DuckDB::fetch_rows(String statement, Array args, int result_type) {
	Array result;

	// // Empty statement
	// if (!statement.strip_edges().length()) {
	// 	return result;
	// }

	// // Cannot prepare query
	// sqlite3_stmt *stmt = prepare(statement.strip_edges().utf8().get_data());
	// if (!stmt) {
	// 	return result;
	// }

	// // Bind arguments
	// if (!bind_args(stmt, args)) {
	// 	sqlite3_finalize(stmt);
	// 	return result;
	// }

	// // Fetch rows
	// while (sqlite3_step(stmt) == SQLITE_ROW) {
	// 	// Do a step
	// 	result.append(parse_row(stmt, result_type));
	// }

	// // Delete prepared statement
	// sqlite3_finalize(stmt);

	// Return the result
	return result;
}

Dictionary DuckDB::parse_row(duckdb_prepared_statement stmt, int result_type) {
	Dictionary result;

	// // Get column count
	// int col_count = sqlite3_column_count(stmt);

	// // Fetch all column
	// for (int i = 0; i < col_count; i++) {
	// 	// Key name
	// 	const char *col_name = sqlite3_column_name(stmt, i);
	// 	String key = String(col_name);

	// 	// Value
	// 	int col_type = sqlite3_column_type(stmt, i);
	// 	Variant value;

	// 	// Get column value
	// 	switch (col_type) {
	// 		case SQLITE_INTEGER:
	// 			value = Variant(sqlite3_column_int(stmt, i));
	// 			break;

	// 		case SQLITE_FLOAT:
	// 			value = Variant(sqlite3_column_double(stmt, i));
	// 			break;

	// 		case SQLITE_TEXT: {
	// 			int size = sqlite3_column_bytes(stmt, i);
	// 			String str = String::utf8((const char *)sqlite3_column_text(stmt, i), size);
	// 			value = Variant(str);
	// 			break;
	// 		}
	// 		case SQLITE_BLOB: {
	// 			PackedByteArray arr;
	// 			int size = sqlite3_column_bytes(stmt, i);
	// 			arr.resize(size);
	// 			memcpy((void *)arr.ptr(), sqlite3_column_blob(stmt, i), size);
	// 			value = Variant(arr);
	// 			break;
	// 		}

	// 		default:
	// 			break;
	// 	}

	// 	// Set dictionary value
	// 	if (result_type == RESULT_NUM)
	// 		result[i] = value;
	// 	else if (result_type == RESULT_ASSOC)
	// 		result[key] = value;
	// 	else {
	// 		result[i] = value;
	// 		result[key] = value;
	// 	}
	// }

	return result;
}

Array DuckDB::fetch_array(String query) {
	return fetch_rows(query, Array(), RESULT_BOTH);
}

Array DuckDB::fetch_array_with_args(String query, Array args) {
	return fetch_rows(query, args, RESULT_BOTH);
}

Array DuckDB::fetch_assoc(String query) {
	return fetch_rows(query, Array(), RESULT_ASSOC);
}

Array DuckDB::fetch_assoc_with_args(String query, Array args) {
	return fetch_rows(query, args, RESULT_ASSOC);
}

DuckDB::~DuckDB() {
	// Close database
	close();
	// Make sure to invalidate all associated queries.
	for (uint32_t i = 0; i < queries.size(); i += 1) {
		DuckDBQuery *query = Object::cast_to<DuckDBQuery>(queries[i]->get_ref());
		if (query != nullptr) {
			query->init(nullptr, "");
		}
	}
}

void DuckDB::_bind_methods() {
	ClassDB::bind_method(D_METHOD("open", "path"), &DuckDB::open);
	ClassDB::bind_method(D_METHOD("open_in_memory"), &DuckDB::open_in_memory);
	ClassDB::bind_method(D_METHOD("open_buffered", "path", "buffers", "size"), &DuckDB::open_buffered);

	ClassDB::bind_method(D_METHOD("close"), &DuckDB::close);

	ClassDB::bind_method(D_METHOD("create_query", "statement"), &DuckDB::create_query);

	ClassDB::bind_method(D_METHOD("query", "statement"), &DuckDB::query);
	ClassDB::bind_method(D_METHOD("query_with_args", "statement", "args"), &DuckDB::query_with_args);
	ClassDB::bind_method(D_METHOD("fetch_array", "statement"), &DuckDB::fetch_array);
	ClassDB::bind_method(D_METHOD("fetch_array_with_args", "statement", "args"), &DuckDB::fetch_array_with_args);
	ClassDB::bind_method(D_METHOD("fetch_assoc", "statement"), &DuckDB::fetch_assoc);
	ClassDB::bind_method(D_METHOD("fetch_assoc_with_args", "statement", "args"), &DuckDB::fetch_assoc_with_args);
}
