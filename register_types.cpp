#include "register_types.h"

#include "core/object/class_db.h"
#include "godot_duckdb.h"

void register_duckdb_types() {
	ClassDB::register_class<DuckDB>();
	ClassDB::register_class<DuckDBQuery>();
}

void unregister_duckdb_types() {
}