import os
from dateutil.parser import parse
from sqlalchemy import create_engine
from unittest import TestCase as TestCaseBase
from dblink import Database, Table
from sqlalchemy.engine.url import make_url
from tests import create_table, drop_table

# DB_URL = 'postgresql+psycopg2://user:psw@host:port/db_name'
DB_URL = 'sqlite:///:memory:'


class TestCase(TestCaseBase):
    def tearDown(self):
        fn = make_url(DB_URL).database
        if os.path.exists(fn):
            os.remove(fn)


class CoreTest(TestCase):
    def setUp(self):
        self.maxDiff = None
        self.length = 100
        self.engine = create_engine(DB_URL)
        create_table(self.engine)

    def tearDown(self):
        drop_table(self.engine)
        super().tearDown()

    def test_A_insert(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'}
            user_table.insert(data)
            result = user_table.query.one()
            answer = {c: getattr(result, c) for c in data.keys()}
            self.assertEqual(answer, data)

    def test_B_update(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'}
            user_table.insert(data)
            data2 = {'id': 1, 'name': 'n', 'fullname': 'f', 'password': 'p'}
            user_table.update(data2, ['id'], ['name', 'password'])
            right = {'id': 1, 'name': 'n', 'fullname': 'f1', 'password': 'p'}
            result = user_table.query.one()
            answer = {c: getattr(result, c) for c in right.keys()}
            self.assertEqual(answer, right)

    def test_B_insert_or_update(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'}
            user_table.insert_or_update(data, ['id'], ['name', 'password'])
            result = user_table.query.one()
            answer = {c: getattr(result, c) for c in data.keys()}
            self.assertEqual(answer, data)

            data2 = {'id': 1, 'name': 'n', 'fullname': 'f', 'password': 'p'}
            user_table.insert_or_update(data2, ['id'], ['name', 'password'])
            result = user_table.query.one()
            right = {'id': 1, 'name': 'n', 'fullname': 'f1', 'password': 'p'}
            answer = {c: getattr(result, c) for c in right.keys()}
            self.assertEqual(answer, right)

    def test_C_bulk_insert(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = [
                {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'},
                {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
            ]
            user_table.bulk_insert(data)
            results = user_table.query.all()
            answer = sorted([
                {c: getattr(i, c) for c in data[0].keys()} for i in results
            ], key=lambda x: x['id'])
            right = sorted(data, key=lambda x: x['id'])
            self.assertEqual(answer, right)

    def test_D_bulk_update(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = [
                {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'},
                {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
            ]
            user_table.bulk_insert(data)
            data2 = [
                {'id': 1, 'name': 'n', 'fullname': 'f', 'password': 'p'},
                {'id': 2, 'name': 'n', 'fullname': 'f', 'password': 'p'},
            ]
            user_table.bulk_update(data2, ['id'],
                                   ['name', 'fullname', 'password'])
            results = user_table.query.all()
            answer = sorted([
                {c: getattr(i, c) for c in data2[0].keys()} for i in results
            ], key=lambda x: x['id'])
            right = sorted(data2, key=lambda x: x['id'])
            self.assertEqual(answer, right)

    def test_E_bulk_update_or_insert(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'}
            user_table.insert(data)
            data2 = [
                {'id': 1, 'name': 'n', 'fullname': 'f', 'password': 'p'},
                {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
            ]
            user_table.bulk_insert_or_update(data2, ['id'],
                                             ['name', 'fullname', 'password'])
            results = user_table.query.all()
            answer = sorted([
                {c: getattr(i, c) for c in data2[0].keys()} for i in results
            ], key=lambda x: x['id'])
            right = sorted(data2, key=lambda x: x['id'])
            self.assertEqual(answer, right)

    def test_F_delete(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = [
                {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'},
                {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
            ]
            user_table.bulk_insert(data)
            user_table.delete(data[0], unique_fields=['id'])
            result = user_table.query.one()
            answer = {c: getattr(result, c) for c in data[1].keys()}
            self.assertEqual(answer, data[1])

    def test_G_bulk_delete(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)

            data = [
                {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'},
                {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
            ]
            user_table.bulk_insert(data)
            user_table.bulk_delete(data, unique_fields=['id'])
            answer = user_table.query.one_or_none()
            self.assertEqual(answer, None)

    def test_H_get_or_insert(self):
        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            user_table = Table('users', db)
            data = {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'}
            result1, flag = user_table.get_or_insert(**data)
            result2 = user_table.query.one()
            answer1 = {c: getattr(result1, c) for c in data.keys()}
            answer2 = {c: getattr(result2, c) for c in data.keys()}
            self.assertEqual(flag, True)
            self.assertEqual(answer1, data)
            self.assertEqual(answer2, data)
            result, flag = user_table.get_or_insert(**data)
            answer = {c: getattr(result, c) for c in data.keys()}
            self.assertEqual(flag, False)
            self.assertEqual(answer, data)

    def test_I_bulk_update_or_insert_with_datetime(self):

        with Database(DB_URL) as db:
            create_table(db.engine)  # For memory sqlite test
            birth_info_table = Table('birth_info', db)

            def _(str_date):
                if db.dialect == 'sqlite':
                    return parse(str_date).date()
                else:
                    return str_date

            data = {'user_id': 1, 'birthday': _('2010-01-01')}
            birth_info_table.insert(data)
            data2 = [
                {'user_id': 1, 'birthday': _('2010-01-01')},
                {'user_id': 2, 'birthday': _('2010-01-02')}
            ]
            birth_info_table.bulk_insert_or_update(
                data2, ['user_id', 'birthday'], ['user_id', 'birthday'])

            results = set(birth_info_table.query
                          .values_list('user_id', flat=True))
            self.assertEqual(results, {1, 2})


class QueryTest(TestCase):
    @staticmethod
    def prepare_data():
        user_data = [
            {'id': 1, 'name': 'n1', 'fullname': 'f1', 'password': 'p1'},
            {'id': 2, 'name': 'n2', 'fullname': 'f2', 'password': 'p2'},
        ]
        address_data = [
            {'id': 1, 'user_id': 1, 'email_address': 'jack@yahoo.com'},
            {'id': 2, 'user_id': 1, 'email_address': 'jack@msn.com'},
            {'id': 3, 'user_id': 2, 'email_address': 'www@www.org'},
            {'id': 4, 'user_id': 2, 'email_address': 'wendy@aol.com'}
        ]
        return user_data, address_data

    def setUp(self):
        self.maxDiff = None
        self.length = 100
        self.db = Database(DB_URL)
        create_table(self.db.engine)
        self.user_table = Table('users', self.db)
        self.address_table = Table('addresses', self.db)
        user_data, address_data = self.prepare_data()
        self.user_table.bulk_insert(user_data)
        self.address_table.bulk_insert(address_data)

    def tearDown(self):
        self.db.close()
        drop_table(self.db.engine)
        super().tearDown()

    def test_A_filter(self):
        all_user = self.user_table.query.filter(id__in=[1, 2]).all()
        answer = self.user_table.query.filter(id=1).one()
        self.assertEqual(len(all_user), 2)
        self.assertIn(answer, all_user)
        self.assertEqual(answer.name, 'n1')

    def test_C_values(self):
        answer = self.user_table.query.filter(id=1).values('id', 'name').one()
        self.assertEqual(tuple(answer), (1, 'n1'))

    def test_D_values_list(self):
        answer = self.user_table.query \
            .filter(id__gte=1).values_list('id', 'name')
        self.assertEqual(sorted(answer),
                         [(1, 'n1'), (2, 'n2')])

    def test_E_join(self):
        user = self.user_table
        address = self.address_table
        answer = user.join(address).values_list(
            user.id, user.fullname, address.email_address)
        right = sorted([
            (1, 'f1', 'jack@yahoo.com'), (1, 'f1', 'jack@msn.com'),
            (2, 'f2', 'www@www.org'), (2, 'f2', 'wendy@aol.com')
        ])
        self.assertEqual(sorted(answer), right)

        answer = user.join(address, 'id', 'user_id') \
            .filter(user_id__in=[1]) \
            .values_list('user_id', user.c.name,
                         address.email_address, 'fullname')
        right = sorted([(1, 'n1', 'jack@yahoo.com', 'f1'),
                        (1, 'n1', 'jack@msn.com', 'f1')])
        self.assertEqual(sorted(answer), right)

    def test_F_delete(self):
        items = self.address_table.query.filter(id=1)
        delete_count = items.delete()
        self.assertEqual(delete_count, 1)
        result = list(self.address_table.query.order_by('-id')
                      .values_list('id', flat=True))

        self.assertEqual(result, [4, 3, 2])

    def test_G_order_by(self):
        answer = self.user_table.query.order_by('-id') \
            .values_list('id', 'name')
        self.assertEqual(list(answer), [(2, 'n2'), (1, 'n1')])

    def test_H_distinct(self):
        answer = self.address_table.query.distinct('user_id') \
            .values_list('user_id', flat=True)
        self.assertEqual(sorted(answer), [1, 2])
        answer = self.address_table \
            .query.values_list('user_id', distinct=True, flat=True)
        self.assertEqual(sorted(answer), [1, 2])

    def test_I_text_query(self):
        result = self.user_table.query.filter('users.id > 1').all()
        self.assertEqual(result, [(2, 'n2', 'f2', 'p2')])

    def test_others(self):
        dialect = make_url(DB_URL).get_dialect().name
        self.assertEqual(self.db.dialect, dialect)
        self.assertIn("Table('users',", self.user_table.description)
