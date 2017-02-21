def make_postgres_read_statement(table, kv_map, keys_to_get, limit, order_by,
                                 order_type, debug, clause, group_by, join_clause):
    _prefix = "SELECT"
    _join_by = " " + join_clause + " "
    _table_string = " ".join(["FROM", table])
    _key_string = _join_by.join([k + clause + "%s" for k in kv_map.keys()])
    statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string])
    if len(kv_map.keys()) > 0:
        statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string, "WHERE", _key_string])
    if group_by is not None:
        statement += " GROUP BY " + group_by
    if order_by is not None:
        statement += " ORDER BY " + order_by + " " + order_type
    if limit is not None:
        statement += " LIMIT " + str(limit)
    if debug:
        print("Reading From Db: %s, %s" % (statement, kv_map.values()))
    return statement, kv_map.values()


def prepare_values(all_values, keys_to_get):
    ret_val = []
    if all_values is None:
        return None
    k = sorted(keys_to_get)
    for row in all_values:
        row_kv = {}
        if len(row) == len(keys_to_get):
            for idx in range(0, len(row)):
                row_kv[k[idx]] = str(row[idx])
        else:
            print("Number of keys to be fetched are not correct")
            continue
        ret_val.append(row_kv)
    return ret_val


class where:
    def __init__(self, kv_map, clause="=", join_clause="and"):
        self.kv_map = kv_map
        self.clause = clause
        self.join_clause = join_clause

    def get_statement(self):
        _join_by = " " + self.join_clause + " "
        _clause = " " + self.clause + " "
        _key_string = _join_by.join([k + _clause + "%s" for k in self.kv_map.keys()])
        return _key_string

    def get_values(self):
        return self.kv_map.values()


def make_postgres_read_statement_new(table, wheres, keys_to_get, limit, order_by,
                                     order_type, debug, group_by, join_clause):
    _prefix = "SELECT"
    _join_by = " " + join_clause + " "
    _table_string = " ".join(["FROM", table])
    for whr in wheres:
        whr.get_statement()
    _key_string = _join_by.join([w.get_statement() for w in wheres])
    statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string])
    if len(wheres) > 0:
        statement = " ".join([_prefix, ", ".join(sorted(keys_to_get)), _table_string, "WHERE", _key_string])
    if group_by is not None:
        statement += " GROUP BY " + group_by
    if order_by is not None:
        statement += " ORDER BY " + order_by + " " + order_type
    if limit is not None:
        statement += " LIMIT " + str(limit)

    values = []
    for w in wheres:
        v = w.get_values()
        values += v
    if debug:
        print("Reading From Db: %s, %s" % (statement, values))
    return statement, values
