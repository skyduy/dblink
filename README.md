# dblink

[![PyPI version](https://badge.fury.io/py/dblink.svg)](https://badge.fury.io/py/dblink)
[![Build Status](https://travis-ci.org/skyduy/dblink.svg?branch=master)](https://travis-ci.org/skyduy/dblink)
[![codecov](https://codecov.io/gh/skyduy/dblink/branch/master/graph/badge.svg)](https://codecov.io/gh/skyduy/dblink)

> Aimed for easily using query, insert, update and delete with an exist table, the filter query syntax likes Django's. You can find out the simple usage in the code below.

Suppose you have two tables: `users` and `addresses` created by:

```sql
CREATE TABLE users (
    id INTEGER NOT NULL,
    name VARCHAR(50),
    fullname VARCHAR(50),
    password VARCHAR(12),
    PRIMARY KEY (id)
);

CREATE TABLE addresses (
    id INTEGER NOT NULL,
    email_address VARCHAR NOT NULL,
    user_id INTEGER,
    PRIMARY KEY (id),
    FOREIGN KEY(user_id) REFERENCES users (id)
);
```

You can link to exist table in database using [Database Urls](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls).

```python
from dblink import Database, Table
db = Database(url='sqlite:///:memory:')
user_table = Table('users', db)
# ...
db.close()

with Database(url='postgresql://scott:tiger@localhost/mydatabase') as db:
    address_table = Table('addresses', db)
    # ...
```

Here is a simple example.

```python
"""
Suppose you have two table: users and addresses.
"""
from dblink import Database, Table


with Database('sqlite:///:memory:') as db:
    table_user = Table('users', db)
    table_address = Table('addresses', db)

    # show description
    print(table_user.description)
    print(table_address.description)

    # chain query, you can call delete on the single table result
    table_user.query.filter(id=1).one_or_none()

    table_user.query.filter(id__gte=2) \
                    .order_by('name') \
                    .values_list('id', 'name')

    table_user.query.filter(id__in=[1, 2, 3]) \
                    .filter(name__startswith='Yu').all()

    table_user.query.order_by('-name') \
                    .values_list('fullname', flat=True, distinct=True)

    table_user.query.distinct('name').values_list('name', flat=True)

    table_user.query.filter(id__in=[1, 2, 3]).delete()

    # join query
    table_user.join(table_address) \
              .filter(id__lt=10000) \
              .filter(email_address__contains='gmail') \
              .filter(**{'addresses.id__gte': 100}) \
              .values_list('user_id', 'name', 'email_address',
                           table_address.id, 'users.fullname')

    # get or insert
    instance, create = table_user.get_or_insert(id=1, name='jone')

    # single record operation.
    table_user.insert({'id': 1, 'name': 'YuJun', 'password': 'psw'})

    table_user.update({'id': 1, 'name': 'skyduy', 'password': 'psw'},
                      unique_fields=['id'], update_fields=['name', 'password'])

    table_user.insert_or_update(
        {'id': 1, 'name': 'skyduy', 'password': 'psw'},
        unique_fields=['id'], update_fields=['name', 'password']
    )

    table_user.delete({'id': 1, 'name': "I don't matter"},
                      unique_fields=['id'])

    # bulk operation
    items = [{'id': 1, 'name': 'yujun', 'password': 'haha'},
             {'id': 2, 'name': 'skyduy', 'password': 'aha'},]
    unique_fields = ['id']
    update_fields = ['name']
    table_user.bulk_insert(items)
    table_user.bulk_delete(items, unique_fields)
    table_user.bulk_update(items, unique_fields, update_fields)
    table_user.bulk_insert_or_update(items, unique_fields, update_fields)
```