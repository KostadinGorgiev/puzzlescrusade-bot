import sqlite3, time
class datebase:
    def __init__(self):
        self.connection = sqlite3.connect("db.db", check_same_thread=False)
        self.cursor = self.connection.cursor()
    async def user_exist(self, chat_id):
        try:
            with self.connection:
                info = self.cursor.execute(f"SELECT * FROM users WHERE chat_id = '{chat_id}'").fetchall()
                return bool(len(info))
        except Exception as ex:
            print(ex)
    async def add_user(self, chat_id):
        try:
            with self.connection:
                return self.cursor.execute(
                    "INSERT INTO 'users' ('chat_id') VALUES (?)",
                    (chat_id,))
        except Exception as e:
            print(e)

    async def taking_exist(self, chat_id):
        try:
            with self.connection:
                for value in self.cursor.execute(f'SELECT taking FROM users WHERE chat_id = "{chat_id}"'):
                    return value[0]
        except Exception as e:
            print(e)

    async def add_taking(self, chat_id, take):
        try:
            with self.connection:
                return self.cursor.execute(f"UPDATE users SET taking = '{str(take)}' WHERE chat_id = '{chat_id}'")
        except Exception as e:
            print(e)

    async def hash_exist(self, chat_id):
        try:
            with self.connection:
                for value in self.cursor.execute(f'SELECT hash FROM users WHERE chat_id = "{chat_id}"'):
                    return value[0]
        except Exception as e:
            print(e)

    async def add_hash(self, chat_id, hash):
        try:
            with self.connection:
                return self.cursor.execute(f"UPDATE users SET hash = '{str(hash)}' WHERE chat_id = '{chat_id}'")
        except Exception as e:
            print(e)
