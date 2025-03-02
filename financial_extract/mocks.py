import constants


class SecretsMock:
    def __init__(self):
        pass

    def access_secret_version(self, name: str):
        pass
        #map = {
        #    f"projects/9876543210/secrets/{constants.SECRET_DATABASE_CREDENTIALS}/versions/latest": {"payload": {"data": '{"host": "127.0.0.1", "database": "postgres", "user": "postgres", "password": "postgres", "port": "5432"}'}}
        #}
        #return map[name] if name in map else None


class StorageMock:
    def __init__(self):
        pass    

class DatabaseMock:
    def __init__(self):
        pass