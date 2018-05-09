import collections
import logging
import sqlalchemy as sal
from functools import wraps
from datetime import date
from dateutil.parser import parse as str2datetime
from sqlalchemy.sql import sqltypes
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import operators, extract
from dblink.exceptions import (
    NoColumns, DuplicateColumns, UnexpectedParam, NoTableError,
)


logger = logging.getLogger('DBLink')
logger.setLevel(logging.WARNING)
ch = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(asctime)s][%(levelname)s][%(name)s]:  %(message)s")
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
logger.addHandler(ch)


def with_transaction(commit=True):
    def decorate(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            try:
                result = f(self, *args, **kwargs)
            except DBAPIError as e:
                msg = ' '.join(['{}'] * len(e.args)).format(*e.args)
                logger.error(msg)
                self.session.rollback()
                e.statement = e.statement[:300]
                e.params = str(e.params)[:300]
                raise
            else:
                if commit is True:
                    self.session.commit()
                return result

        return wrapper
    return decorate


class Database:
    def __init__(self, url, encoding='utf8'):
        self.__engine = sal.create_engine(
            url, encoding=encoding, pool_pre_ping=True)
        self.__metadata = sal.MetaData(bind=self.__engine)
        self.__session = scoped_session(sessionmaker(
            autocommit=False, autoflush=True, bind=self.__engine))()
        self.__open = True

    @property
    def dialect(self):
        return self.engine.dialect.name

    @property
    def engine(self):
        return self.__engine

    @property
    def session(self):
        return self.__session

    @property
    def open(self):
        return self.__open

    @property
    def name(self):
        return self.engine.url.database

    @property
    def metadata(self):
        return self.__metadata

    def close(self):
        self.session.close()
        self.engine.dispose()
        self.__open = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Table:
    def __init__(self, name, db):
        if not isinstance(db, Database) or db.open is False:
            msg = 'Invalid db or db has been closed'
            raise UnexpectedParam(msg)
        self.__db = db
        self.__table = self._link_table(name)

    def _link_table(self, name):
        if not self.__db.engine.has_table(name):
            msg = "Database [{}] has no such table [{}]".format(
                self.__db.name, name)
            raise NoTableError(msg)
        return sal.Table(name, self.__db.metadata, autoload=True)

    # static property
    @property
    def name(self):
        return self.__table.name

    @property
    def indexes(self):
        return list(self.__table.indexes)

    @property
    def columns(self):
        return list(self.__table.columns)

    @property
    def description(self):
        return self.__table.__repr__()

    @property
    def sal_table(self):
        return self.__table

    @property
    def db(self):
        return self.__db

    @property
    def session(self):
        return self.db.session

    # query
    @property
    def query(self):
        if not hasattr(self, '__query_object'):
            query = self.session.query(self.sal_table)
            params = {'table_name2table': {self.name: self.sal_table},
                      'column_name2tables': {c.name: {self.sal_table}
                                             for c in self.columns}}
            query_object = Query(self.session, query, **params)
            setattr(self, '__query_object', query_object)
        return getattr(self, '__query_object')

    def join(self, *args, **kwargs):
        return self.query.join(*args, **kwargs)

    # insert / update / delete
    def get_or_insert(self, **kwargs):
        instance, create = self.query.filter(**kwargs).one_or_none(), False
        if instance is None:
            self.insert(kwargs)
            instance, create = self.query.filter(**kwargs).one(), True
        return instance, create

    def insert(self, item):
        if not item:
            return
        return self.bulk_insert([item])

    def delete(self, item, unique_fields):
        if not item:
            return
        return self.bulk_delete([item], unique_fields)

    def update(self, item, unique_fields, update_fields):
        if not item:
            return
        return self.bulk_update([item], unique_fields, update_fields)

    def insert_or_update(self, item, unique_fields, update_fields):
        return self.bulk_insert_or_update(
            [item], unique_fields, update_fields)

    @with_transaction()
    def bulk_insert(self, data):
        if not data:
            return
        return self.session.execute(self.sal_table.insert(), data)

    @with_transaction()
    def bulk_delete(self, data, unique_fields):
        if not data:
            return

        cond_args = [getattr(self.c, f) == bindparam('old_{}'.format(f))
                     for f in unique_fields]
        new_data = [{'old_{}'.format(f): item[f] for f in unique_fields}
                    for item in data]

        stmt = self.sal_table.delete().where(sal.and_(*cond_args))
        return self.session.execute(stmt, new_data)

    @with_transaction()
    def bulk_update(self, data, unique_fields, update_fields):
        if not data:
            return
        unique_fields, update_fields = set(unique_fields), set(update_fields)
        if (unique_fields | update_fields) - {c.name for c in self.c}:
            raise ValueError('Fields contain invalid column')

        update_fields = update_fields - unique_fields
        cond_args = [getattr(self.c, f) == bindparam('old_{}'.format(f))
                     for f in unique_fields]
        update_kwargs = {f: bindparam('new_{}'.format(f))
                         for f in update_fields}

        new_data = list()
        for item in data:
            cond_item = {'old_{}'.format(f): item[f] for f in unique_fields}
            update_item = {'new_{}'.format(f): item[f] for f in update_fields}
            new_item = dict(collections.ChainMap(cond_item, update_item))
            new_data.append(new_item)

        stmt = self.sal_table.update() \
            .where(sal.and_(*cond_args)) \
            .values(**update_kwargs)

        return self.session.execute(stmt, new_data)

    @with_transaction()
    def bulk_insert_or_update(self, data, unique_fields, update_fields):
        if not data:
            return
        unique_fields, update_fields = set(unique_fields), set(update_fields)

        field2type = {c.name: c.type for c in self.c}
        if (unique_fields | update_fields) - field2type.keys():
            raise ValueError('Fields contain invalid column')

        unique_fields, update_fields = list(unique_fields), list(update_fields)

        unique2data = dict()
        for item in data:
            tmp = list()
            for field in unique_fields:
                value = item[field]
                if isinstance(field2type[field], sqltypes.DATE):
                    value = value if isinstance(value, date) else \
                        str2datetime(value).date()
                tmp.append(value)
            unique2data[tuple(tmp)] = item

        query = dict(zip(["{}__in".format(u) for u in unique_fields],
                         (set(i) for i in zip(*unique2data.keys()))))

        update_data = []
        for d in self.query.filter(**query):
            key = tuple(getattr(d, u) for u in unique_fields)
            if key in unique2data:
                update_data.append(unique2data.pop(key))
        create_data = list(unique2data.values())
        self.bulk_insert(create_data)
        if set(unique_fields) != set(update_fields):
            self.bulk_update(update_data, unique_fields, update_fields)

    @property
    def c(self):
        return self.sal_table.c

    def __getattr__(self, item):
        return getattr(self.c, item)


class Query:
    WARNING_LEN = 2000
    _underscore_operators = {
        'gt': operators.gt,
        'lt': operators.lt,
        'lte': operators.le,
        'gte': operators.ge,
        'le': operators.le,
        'ge': operators.ge,
        'in': operators.in_op,
        'contains': operators.contains_op,
        'exact': operators.eq,
        'startswith': operators.startswith_op,
        'endswith': operators.endswith_op,

        'range': lambda c, x: operators.between_op(c, x[0], x[1]),

        'year': lambda c, x: extract('year', c) == x,
        'month': lambda c, x: extract('month', c) == x,
        'day': lambda c, x: extract('day', c) == x,
    }

    def __init__(self, session, query, **kwargs):
        self.session = session
        self.query = query
        self.table_name2table = kwargs.get('table_name2table', {})
        self.column_name2tables = kwargs.get('column_name2tables', {})

    def _copy_column_name2tables(self):
        result = dict()
        for cname, table_set in self.column_name2tables.items():
            result.setdefault(cname, set())
            for table in table_set:
                result[cname].add(table)
        return result

    def join(self, table, column1=None, column2=None):
        assert isinstance(table, Table), UnexpectedParam
        automap_base(metadata=table.db.metadata).prepare()
        right_table = table.sal_table

        if (column1 is None) ^ (column2 is None):
            msg = 'Column1 and Column2 must be all None or all not Null'
            raise UnexpectedParam(msg)
        if column1 is None:
            query = self.query.join(right_table)
        else:
            if isinstance(column1, str):
                column1 = self._parse_column(column1)
            else:
                assert isinstance(column1, sal.Column), UnexpectedParam
            if isinstance(column2, str):
                column2 = getattr(right_table.c, column2)
            else:
                assert column2 in set(right_table.c), UnexpectedParam
            query = self.query.join(right_table, column1 == column2)
        t2t_copy = self.table_name2table.copy()
        c2t_copy = self._copy_column_name2tables()
        t2t_copy[right_table.name] = right_table
        for column in right_table.columns:
            c2t_copy.setdefault(column.name, set()).add(right_table)
        return self._clone(query=query, table_name2table=t2t_copy,
                           column_name2tables=c2t_copy)

    def filter(self, *args, **kwargs):
        return self._filter_or_exclude(False, *args, **kwargs)

    def exclude(self, *args, **kwargs):
        return self._filter_or_exclude(True, *args, **kwargs)

    def _filter_or_exclude(self, negate, *args, **kwargs):
        conds = self._parse_cond(args, kwargs)
        query = (self.query.filter(~sal.and_(*conds)) if negate else
                 self.query.filter(*conds))
        return self._clone(query=query)

    def _parse_cond(self, args, kwargs):
        conditions = []
        for txt in args:
            conditions.append(sal.text(txt))
        for arg, value in kwargs.items():
            name, op = arg, 'exact'
            if '__' in arg:
                name, op = arg.split('__')
                if op not in self._underscore_operators:
                    msg = "not support this type {}".format(op)
                    raise KeyError(msg)
            if op == 'in' and len(value) > self.WARNING_LEN and \
                    self.session.bind.dialect.name == 'mysql':
                msg = 'In clause is longer than {}, may cause ' \
                      'MySQL server gone away.'.format(self.WARNING_LEN)
                logger.warning(msg)
            op_func = self._underscore_operators[op]
            conditions.append(op_func(self._parse_column(name), value))
        return conditions

    def _parse_column(self, column):
        if isinstance(column, sal.Column):
            return column
        if '.' in column:
            table_name, column = column.split('.')
            table = self.table_name2table[table_name]
        else:
            if column not in self.column_name2tables:
                msg = "No column {} appeared in tables".format(column)
                raise NoColumns(msg)
            tables = self.column_name2tables[column]
            if len(tables) > 1:
                msg = "Ambiguous column {}. Please specify table name" \
                    .format(column)
                raise DuplicateColumns(msg)
            table = list(tables)[0]
        return getattr(table.c, column)

    def values(self, *fields, **args):
        origins = [self._parse_column(cname) for cname in fields]
        origins.extend([func(self._parse_column(c))
                        for c, func in args.items()])
        return self._clone(query=self.query.with_entities(*origins))

    def values_list(self, *fields, distinct=False, flat=False, **kwargs):
        clone = self.values(*fields, **kwargs)
        real_flat = flat and (len(fields) + len(kwargs)) == 1

        if distinct:
            clone = clone.distinct()
        return (x[0] if real_flat else x for x in clone)

    def distinct(self, *field_names):
        return self._clone(query=self.query.distinct(*field_names))

    def order_by(self, *args):
        conditions = []
        for arg in args:
            cname, asc = arg, True
            if arg[0] == '-':
                cname, asc = arg[1:], False
            column = self._parse_column(cname)
            conditions.append(column if asc else column.desc())
        return self._clone(query=self.query.order_by(*conditions))

    @with_transaction()
    def delete(self):
        return self.query.delete(synchronize_session=False)

    @with_transaction(commit=False)
    def __getattr__(self, item):
        if item in {'one', 'one_or_none', 'scalar', 'first', 'all'}:
            return getattr(self.query, item)
        raise AttributeError(item)

    @with_transaction(commit=False)
    def __iter__(self):
        return iter(self.query)

    def _clone(self, **kwargs):
        params = {'session': self.session,
                  'query': self.query,
                  'table_name2table': self.table_name2table,
                  'column_name2tables': self.column_name2tables}
        params.update(**kwargs)
        return Query(**params)
