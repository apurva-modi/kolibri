from sqlite3 import OperationalError

from django.conf import settings
from django.db import connection

from kolibri.core.device.models import DummyModel


class DummyOperation(object):
    def __init__(self):
        self.obj = None

    def execute(self):
        self.obj = DummyModel.objects.create()

    def revert(self):
        self.obj.delete()


class PostgresLock(object):
    def __init__(self, key=None):
        self.key = key

    def execute(self):
        query = "SELECT pg_advisory_lock({key}) AS lock;".format(key=self.key)
        with connection.cursor() as c:
            c.execute(query)

    def revert(self):
        query = "SELECT pg_advisory_unlock({key}) AS lock;".format(key=self.key)
        with connection.cursor() as c:
            c.execute(query)


class DatabaseLock(object):
    def _sqlite_operation(self):
        return DummyOperation()

    def _postgresql_operation(self):
        return PostgresLock(self.lock_id)

    def _not_implemented_operation(self):
        raise NotImplementedError(
            "Operation not implemented for {vendor}".format(vendor=connection.vendor)
        )

    def _get_operation(self):
        return getattr(
            self,
            "_{db_type}_operation".format(db_type=self.db_type),
            self._not_implemented_operation,
        )()

    def __init__(self):
        self.lock_id = settings.TASK_LOCK_ID or 1
        self.db_type = connection.vendor
        self.operation = self._get_operation()

    def _wait_sqlite(self):
        while True:
            try:
                self.operation.execute()
                break
            except OperationalError as e:
                if "database is locked" not in str(e):
                    raise e

    def _wait_postgresql(self):
        self.operation.execute()

    def _wait_not_implemented(self):
        raise NotImplementedError(
            "kolibri.core.utils.cache.DatabaseLock not implemented for vendor {vendor}".format(
                vendor=connection.vendor
            )
        )

    def __enter__(self):
        getattr(
            self,
            "_wait_{db_type}".format(db_type=self.db_type),
            self._wait_not_implemented,
        )()

    def __exit__(self):
        if self.operation:
            self.operation.revert()
