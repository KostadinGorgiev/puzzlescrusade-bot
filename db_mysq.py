import mysql.connector
import pytz
from mysql.connector import Error
from datetime import datetime

class database:
    def __init__(self, host, user, password, database):
        """Initialize connection to MySQL database."""
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def _parse_range_filter(self, value):
        """
        Parse a range filter from strings like "1-5", "10+", "<100K".

        Args:
            value (str): The range string.

        Returns:
            tuple: A tuple of (min_value, max_value) or (None, None) if not valid.
        """
        if "+" in value:
            min_value = self._parse_numeric_value(value.replace("+", ""))
            return (min_value, None)
        elif "-" in value:
            min_value, max_value = map(self._parse_numeric_value, value.split("-"))
            return (min_value, max_value)
        elif value.startswith("<"):
            max_value = self._parse_numeric_value(value[1:])
            return (None, max_value)
        else:
            exact_value = self._parse_numeric_value(value)
            return (exact_value, exact_value)

    def _parse_numeric_value(self, value):
        """
        Parse a numeric value from a string with suffixes like K, M, B, or a '+' symbol.

        Args:
            value (str): The numeric string.

        Returns:
            int: The parsed integer value.
        """
        multipliers = {
            "K": 1_000,
            "M": 1_000_000,
            "B": 1_000_000_000,
        }
        if value.endswith("+"):
            value = value[:-1]
        if value[-1] in multipliers:
            return int(value[:-1]) * multipliers[value[-1]]
        return int(value)

    def _parse_levels_to_balance(self, levels):
        """
        Converts level numbers to balance ranges based on predefined level mapping.
        """
        level_mapping = [
            {"title": "Level 1", "from": 0, "to": 5000},
            {"title": "Level 2", "from": 5000, "to": 25000},
            {"title": "Level 3", "from": 25000, "to": 125000},
            {"title": "Level 4", "from": 125000, "to": 1000000},
            {"title": "Level 5", "from": 1000000, "to": 2000000},
            {"title": "Level 6", "from": 2000000, "to": 10000000},
            {"title": "Level 7", "from": 10000000, "to": 50000000},
            {"title": "Level 8", "from": 50000000, "to": 250000000},
            {"title": "Level 9", "from": 250000000, "to": 1000000000},
            {"title": "Level 10", "from": 1000000000, "to": 10000000000}
        ]
        ranges = []
        for level in levels:
            try:
                level = int(level)
                if 1 <= level <= 10:
                    level_range = level_mapping[level - 1]
                    ranges.append((level_range["from"], level_range["to"]))
            except ValueError:
                continue
        return ranges

    async def get_all_users_activity(self):
        """Fetch all users and their last activity timestamps."""
        try:
            with self.connection.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT t_user_id, updatedAt, last_notified FROM Users")
                users = cursor.fetchall()
            self.connection.commit()
            return users
        except Error as e:
            print(e)
            return None

    async def update_last_notified(self, user_id):
        """Update last notified timestamp for a user."""
        try:
            current_time = datetime.now(pytz.utc)
            with self.connection.cursor() as cursor:
                cursor.execute("UPDATE Users SET last_notified = %s WHERE t_user_id = %s", (current_time, user_id))
            self.connection.commit()
        except Error as e:
            print(e)

    async def get_notifications(self, without_id = False):
        """Fetch all notifications and group them by time."""
        try:
            with self.connection.cursor(dictionary=True) as cursor:

                cursor.execute("SELECT id, time, text FROM notification")
                rows = cursor.fetchall()

            notifications = {}
            for row in rows:
                time = row['time']
                text = row['text']
                id = row['id']
                if time not in notifications:
                    notifications[time] = []
                if without_id:
                    notifications[time].append(text)
                else:
                    notifications[time].append({'text': text,
                                            'id': id})

            return notifications
        except Error as e:
            return {}

    async def get_notification(self, id):
        """Fetch all notifications and group them by time."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT time, text FROM notification WHERE id = '{id}'")
                data = cursor.fetchall()[0]
                return {
                    "id": id,
                    "time": data[0],
                    "text": data[1]
                }
        except Exception as ex:
            return {}
    async def rem_notification(self, notification_id):
        """Remove a notification by ID."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM notification WHERE id = %s", (notification_id,))
            self.connection.commit()
            return True
        except Error as e:
            return False
    async def add_notification(self, time, text):
        """Add a new notification to the database."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO notification (time, text) VALUES (%s, %s)",
                    (time, text),
                )
            self.connection.commit()
        except Error:
            return False

    async def get_users_by_filters(self, filters=None):
        """
        Fetch users from the database based on provided filters, including cards and referrals count.
        """
        try:
            with self.connection.cursor(dictionary=True) as cursor:
                query = """
                    SELECT 
                        u.id, 
                        u.t_user_id, 
                        u.level_point, 
                        u.coin_balance, 
                        COUNT(DISTINCT c.id) AS card_count,
                        COUNT(DISTINCT r.id) AS referral_count,
                        u.createdAt
                    FROM Users u
                    LEFT JOIN Cards c ON u.id = c.user_id
                    LEFT JOIN Referrals r ON u.id = r.user_id
                """
                conditions = []
                having_conditions = []
                params = []

                if filters and "level_point" in filters and filters["level_point"]:
                    level_ranges = self._parse_levels_to_balance(filters["level_point"])
                    level_conditions = [
                        "(u.level_point BETWEEN %s AND %s)" for _ in level_ranges
                    ]
                    for level_range in level_ranges:
                        params.extend(level_range)
                    if level_conditions:
                        conditions.append(f"({' OR '.join(level_conditions)})")

                if filters and "coin_balance" in filters and filters["coin_balance"]:
                    balance_filter = filters["coin_balance"][0]
                    min_balance, max_balance = self._parse_range_filter(balance_filter)
                    if min_balance is not None and max_balance is not None:
                        conditions.append("(u.coin_balance BETWEEN %s AND %s)")
                        params.extend([min_balance, max_balance])
                    elif max_balance is not None:
                        conditions.append("u.coin_balance <= %s")
                        params.append(max_balance)
                    elif min_balance is not None:
                        conditions.append("u.coin_balance >= %s")
                        params.append(min_balance)

                if filters and "createdAt" in filters and filters["createdAt"]:
                    created_at_conditions = []
                    for created_at_filter in filters["createdAt"]:
                        try:
                            if created_at_filter == '<24H':
                                created_at_conditions.append("u.createdAt >= DATE_SUB(NOW(), INTERVAL 1 DAY)")

                            elif '-' in created_at_filter and 'Days' in created_at_filter:
                                min_days, max_days = map(int, created_at_filter.replace('Days', '').split('-'))
                                if 0 <= min_days <= max_days:
                                    created_at_conditions.append(
                                        "u.createdAt BETWEEN DATE_SUB(NOW(), INTERVAL %s DAY) AND DATE_SUB(NOW(), INTERVAL %s DAY)"
                                    )
                                    params.extend([max_days, min_days])

                            elif created_at_filter.endswith("Days") and created_at_filter[:-4].isdigit():
                                exact_day = int(created_at_filter.replace("Days", ""))
                                if exact_day > 0:
                                    created_at_conditions.append(
                                        "u.createdAt BETWEEN DATE_SUB(NOW(), INTERVAL %s DAY) AND DATE_SUB(NOW(), INTERVAL %s DAY)"
                                    )
                                    params.extend([exact_day, exact_day - 1])

                            elif created_at_filter.endswith("Days+") and created_at_filter[:-5].isdigit():
                                min_days = int(created_at_filter.replace('Days+', ''))
                                if min_days > 0:
                                    created_at_conditions.append("u.createdAt <= DATE_SUB(NOW(), INTERVAL %s DAY)")
                                    params.append(min_days)

                        except ValueError:
                            print(f"Ignored invalid createdAt filter: {created_at_filter}")
                            continue

                    if created_at_conditions:
                        conditions.append(f"({' OR '.join(created_at_conditions)})")

                if filters and "cards" in filters and filters["cards"]:
                    cards_filter = filters["cards"][0]
                    min_cards, max_cards = self._parse_range_filter(cards_filter)
                    if min_cards is not None and max_cards is not None:
                        having_conditions.append("card_count BETWEEN %s AND %s")
                        params.extend([min_cards, max_cards])
                    elif min_cards is not None:
                        having_conditions.append("card_count >= %s")
                        params.append(min_cards)
                    elif max_cards is not None:
                        having_conditions.append("card_count <= %s")
                        params.append(max_cards)

                if filters and "referrals" in filters and filters["referrals"]:
                    referrals_filter = filters["referrals"][0]
                    min_referrals, max_referrals = self._parse_range_filter(referrals_filter)

                    if min_referrals is not None and max_referrals is not None:
                        having_conditions.append("referral_count BETWEEN %s AND %s")
                        params.extend([min_referrals, max_referrals])
                    elif min_referrals is not None:  # Handle "1+"
                        having_conditions.append("referral_count >= %s")
                        params.append(min_referrals)
                    elif max_referrals is not None:  # Handle "<100"
                        having_conditions.append("referral_count <= %s")
                        params.append(max_referrals)

                if conditions:
                    query += " WHERE " + " AND ".join(conditions)

                query += " GROUP BY u.id, u.t_user_id, u.level_point, u.coin_balance, u.createdAt"

                if having_conditions:
                    query += " HAVING " + " AND ".join(having_conditions)

                cursor.execute(query, params)
                users = cursor.fetchall()

            self.connection.commit()
            return users

        except Error as e:
            print(f"Error fetching users: {e}")
            return []


if __name__ == '__main__':
    import asyncio

    filters = {
        "level_point": [],
        "coin_balance": [],
        "createdAt": [],
        "cards": [],
        "referrals": []

    }
    db = database()
    print(asyncio.run(db.get_users_by_filters(filters)))
    # print(asyncio.run((db.get_notifications(without_id=True))))