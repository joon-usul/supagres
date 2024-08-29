import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Union, Tuple

class SupagresClient:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432, timeout: int = 30):
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            options=f'-c statement_timeout={timeout * 1000}'  # Convert seconds to milliseconds
        )
        self.conn.autocommit = True

    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    def table(self, table_name: str) -> 'Table':
        return Table(self.conn, table_name)

class Table:
    def __init__(self, conn, table_name: str):
        self.conn = conn
        self.schema = None
        self.table_name = table_name
        if '.' in table_name:
            self.schema, self.table_name = table_name.split('.', 1)
        self._filters = []
        self._select_columns = ['*']
        self._order_by = []
        self._limit = None
        self._offset = None

    def select(self, *columns: str) -> 'Table':
        self._select_columns = list(columns) if columns else ['*']
        return self

    def eq(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '=', value)

    def neq(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '!=', value)

    def gt(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '>', value)

    def gte(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '>=', value)

    def lt(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '<', value)

    def lte(self, column: str, value: Any) -> 'Table':
        return self._add_filter(column, '<=', value)

    def like(self, column: str, pattern: str) -> 'Table':
        return self._add_filter(column, 'LIKE', pattern)

    def ilike(self, column: str, pattern: str) -> 'Table':
        return self._add_filter(column, 'ILIKE', pattern)

    def in_(self, column: str, values: List[Any]) -> 'Table':
        return self._add_filter(column, 'IN', tuple(values))

    def not_in(self, column: str, values: List[Any]) -> 'Table':
        return self._add_filter(column, 'NOT IN', tuple(values))

    def is_null(self, column: str) -> 'Table':
        return self._add_filter(column, 'IS', None)

    def is_not_null(self, column: str) -> 'Table':
        return self._add_filter(column, 'IS NOT', None)

    def _add_filter(self, column: str, operator: str, value: Any) -> 'Table':
        self._filters.append((column, operator, value))
        return self

    def order(self, column: str, ascending: bool = True) -> 'Table':
        self._order_by.append((column, 'ASC' if ascending else 'DESC'))
        return self

    def limit(self, limit: int) -> 'Table':
        self._limit = limit
        return self

    def offset(self, offset: int) -> 'Table':
        self._offset = offset
        return self

    def execute(self) -> List[Dict[str, Any]]:
        if self._select_columns == ['*']:
            select_clause = sql.SQL('*')
        else:
            select_clause = sql.SQL(', ').join(map(sql.Identifier, self._select_columns))

        query_parts = [
            sql.SQL("SELECT {} FROM").format(select_clause),
            self._get_table_identifier(),
            self._build_where_clause(),
            self._build_order_by_clause(),
            self._build_limit_clause(),
            self._build_offset_clause()
        ]

        query = sql.SQL(' ').join(filter(None, query_parts))

        params = self._get_filter_values()
        if self._limit is not None:
            params.append(self._limit)
        if self._offset is not None:
            params.append(self._offset)

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
            except psycopg2.errors.QueryCanceled:
                raise TimeoutError("Query execution timed out")

    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if isinstance(data, dict):
            data = [data]

        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]

        query = sql.SQL("INSERT INTO {} ({}) VALUES {} RETURNING *").format(
            self._get_table_identifier(),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(data))
        )

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, [tuple(row) for row in values])
            return cur.fetchall()

    def upsert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], conflict_columns: List[str]) -> List[Dict[str, Any]]:
        if isinstance(data, dict):
            data = [data]

        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]

        update_columns = [col for col in columns if col not in conflict_columns]

        query = sql.SQL("INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {} RETURNING *").format(
            self._get_table_identifier(),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(data)),
            sql.SQL(', ').join(map(sql.Identifier, conflict_columns)),
            sql.SQL(', ').join(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in update_columns)
        )

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, [tuple(row) for row in values])
            return cur.fetchall()

    def update(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        set_items = [sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in data.keys()]
        query = sql.SQL("UPDATE {} SET {} {} RETURNING *").format(
            self._get_table_identifier(),
            sql.SQL(', ').join(set_items),
            self._build_where_clause()
        )

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, list(data.values()) + self._get_filter_values())
            return cur.fetchall()

    def delete(self) -> List[Dict[str, Any]]:
        query = sql.SQL("DELETE FROM {} {} RETURNING *").format(
            self._get_table_identifier(),
            self._build_where_clause()
        )

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, self._get_filter_values())
            return cur.fetchall()

    def _get_table_identifier(self) -> sql.Composed:
        if self.schema:
            return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.table_name))
        return sql.Identifier(self.table_name)

    def _build_where_clause(self) -> Union[sql.Composed, sql.SQL]:
        if not self._filters:
            return sql.SQL('')

        conditions = []
        for column, operator, value in self._filters:
            if operator.upper() in ('IN', 'NOT IN'):
                placeholders = sql.SQL(', ').join(sql.Placeholder() * len(value))
                condition = sql.SQL("{} {} ({})").format(
                    sql.Identifier(column),
                    sql.SQL(operator),
                    placeholders
                )
            elif value is None and operator.upper() in ('IS', 'IS NOT'):
                condition = sql.SQL("{} {} NULL").format(
                    sql.Identifier(column),
                    sql.SQL(operator)
                )
            else:
                condition = sql.SQL("{} {} {}").format(
                    sql.Identifier(column),
                    sql.SQL(operator),
                    sql.Placeholder()
                )
            conditions.append(condition)

        return sql.SQL(" WHERE {}").format(sql.SQL(" AND ").join(conditions))

    def _build_order_by_clause(self) -> Union[sql.Composed, sql.SQL]:
        if not self._order_by:
            return sql.SQL('')

        order_terms = [sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(direction)) 
                       for col, direction in self._order_by]
        return sql.SQL(" ORDER BY {}").format(sql.SQL(", ").join(order_terms))

    def _build_limit_clause(self) -> Union[sql.Composed, sql.SQL]:
        return sql.SQL(" LIMIT %s") if self._limit is not None else sql.SQL('')

    def _build_offset_clause(self) -> Union[sql.Composed, sql.SQL]:
        return sql.SQL(" OFFSET %s") if self._offset is not None else sql.SQL('')

    def _get_filter_values(self) -> List[Any]:
        values = []
        for _, operator, value in self._filters:
            if operator.upper() in ('IN', 'NOT IN'):
                values.extend(value)
            elif value is not None or operator.upper() not in ('IS', 'IS NOT'):
                values.append(value)
        return values

class RPC:
    def __init__(self, conn):
        self.conn = conn

    def call(self, procedure: str, params: Dict[str, Any] = None) -> Any:
        params = params or {}
        query = sql.SQL("SELECT {}({})").format(
            sql.Identifier(procedure),
            sql.SQL(', ').join([sql.SQL("{}:={}").format(sql.Identifier(k), sql.Placeholder()) for k in params.keys()])
        )

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, list(params.values()))
            result = cur.fetchone()
            return result[procedure] if result else None
