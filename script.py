import pymysql
import os
from config import user, password, host, db_name
import platform

def clear_console():
    system_platform = platform.system()
    if system_platform == "Windows":
        os.system('cls')
    else:
        os.system('clear')

class DatabaseConnection:
    def __init__(self, host, user, password, db_name, port=25565):
        self.host = host
        self.user = user
        self.password = db_name
        self.db_name = db_name
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name,
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.MySQLError as e:
            print(f"Error connecting to the database: {e}")
            self.connection = None

    def disconnect(self):
        if self.connection:
            self.connection.close()

class QueryFactory:
    @staticmethod
    def create_query(case):
        queries = {
            1: "SELECT * FROM {table}",
            2: "SELECT COUNT(*) AS Загальна_Кількість_Транспорту FROM Transport;",
            3: """SELECT T.type, COUNT(*) AS Кількість_Транспорту 
                   FROM Transport_type T 
                   JOIN Transport TR ON T.transport_type_id = TR.trans_type_id 
                   GROUP BY T.type;""",
            5: "SELECT model, COUNT(*) AS Кількість FROM Transport GROUP BY model;",
            6: "SELECT * FROM Transport WHERE number LIKE 'АМ%';",
            7: """SELECT TR.number AS Номер_машини, BS.squad_id AS АйдіСкладу, 
                          E1.first_name AS Імя_Водія, E1.second_name AS Прізвище_Водія, 
                          E1.sex AS Стать_водія, T1.post AS Роль, 
                          E2.first_name AS Імя_Прибиральника, E2.second_name AS Прізвище_Прибиральника, 
                          E2.sex AS Стать_Прибиральника, T2.post AS Посада, 
                          E3.first_name AS Імя_Механіка, E3.second_name AS Прізвище_Механіка, 
                          E3.sex AS Стать_Механіка, T3.post AS Посада 
                   FROM Bort_squad BS 
                   JOIN Employees E1 ON BS.driver = E1.employee_id 
                   JOIN Employees E2 ON BS.cleaner = E2.employee_id 
                   JOIN Employees E3 ON BS.mechanic = E3.employee_id 
                   JOIN Title T1 ON E1.title_id = T1.title_id 
                   JOIN Title T2 ON E2.title_id = T2.title_id 
                   JOIN Title T3 ON E3.title_id = T3.title_id 
                   JOIN Route R ON BS.squad_id = R.bort_squad_id 
                   JOIN Transport TR ON R.transport_id = TR.transport_id;""",
            8: """SELECT DISTINCT E.first_name AS Імя_Водія, E.second_name AS Прізвище_Водія, 
                          E.sex AS Стать_Водія, TR.number AS Номер_Машини, 
                          RG.route_numb AS Номер_Маршруту 
                   FROM Employees E 
                   JOIN Title T ON E.title_id = T.title_id 
                   JOIN Bort_squad BS ON E.employee_id = BS.driver 
                   JOIN Route R ON BS.squad_id = R.bort_squad_id 
                   JOIN Route_Geo_info RG ON R.route_numb_id = RG.route_numb_id 
                   JOIN Transport TR ON R.transport_id = TR.transport_id 
                   WHERE RG.start_point = 'Гідропарк';"""
        }
        return queries.get(case, '')

class Command:
    def __init__(self, connection):
        self.connection = connection

    def execute(self):
        raise NotImplementedError("Subclasses should implement this!")

class SelectAllCommand(Command):
    def __init__(self, connection, table):
        super().__init__(connection)
        self.table = table

    def execute(self):
        if not self.connection:
            print("No database connection.")
            return
        query = QueryFactory.create_query(1).format(table=self.table)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                for row in result:
                    print(row)
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")

class TransportCountCommand(Command):
    def execute(self):
        if not self.connection:
            print("No database connection.")
            return
        query = QueryFactory.create_query(2)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                print(result)
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")

class TransportTypeCountCommand(Command):
    def execute(self):
        if not self.connection:
            print("No database connection.")
            return
        query = QueryFactory.create_query(3)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                for row in result:
                    print(row)
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")

class UpdateSalaryCommand(Command):
    def __init__(self, connection, percentage):
        super().__init__(connection)
        self.percentage = percentage

    def execute(self):
        if not self.connection:
            print("No database connection.")
            return
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CALL IncreaseSalary('{self.percentage}');")
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")

class SortCommand(Command):
    def __init__(self, connection, table, column, order):
        super().__init__(connection)
        self.table = table
        self.column = column
        self.order = order

    def execute(self):
        if not self.connection:
            print("No database connection.")
            return
        try:
            with self.connection.cursor() as cursor:
                query = f"SELECT * FROM {self.table} ORDER BY {self.column} {self.order}"
                cursor.execute(query)
                result = cursor.fetchall()
                for row in result:
                    print(row)
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")

class InputHandler:
    @staticmethod
    def get_table_choice():
        print("1 - Employees\n2 - bort-squad\n3 - route_geo_info\n4 - title\n5 - transport\n6 - transport_type\n7 - route")
        while True:
            try:
                choice = int(input("Введіть номер таблиці: "))
                if 1 <= choice <= 7:
                    return choice
                else:
                    print("Будь ласка, введіть число від 1 до 7.")
            except ValueError:
                print("Будь ласка, введіть дійсний номер.")

    @staticmethod
    def get_percentage():
        while True:
            try:
                percentage = float(input("Введіть відсоток: "))
                return percentage
            except ValueError:
                print("Будь ласка, введіть дійсний відсоток.")

    @staticmethod
    def get_sort_order():
        print("1 - ASC\n2 - DESC")
        while True:
            try:
                choice = int(input("Виберіть порядок сортування (1 або 2): "))
                if choice in (1, 2):
                    return choice
                else:
                    print("Будь ласка, введіть 1 або 2.")
            except ValueError:
                print("Будь ласка, введіть дійсний номер.")

def main():
    connection = DatabaseConnection(host, user, password, db_name)
    connection.connect()

    while True:
        clear_console()
        user_choice = int(input(
            "Виберіть дію:\n1 - Вивести всі дані з таблиці\n2 - Порахувати кількість транспорту\n"
            "3 - Порахувати кількість транспорту за типами\n10 - Підняти зарплату працівникам на N%\n"
            "14 - Відсортувати дані в таблиці\n0 - Exit\n"
        ))

        if user_choice == 0:
            break

        command = None

        if user_choice == 1:
            table_index = InputHandler.get_table_choice()
            table_map = {1: 'employees', 2: 'bort_squad', 3: 'route_geo_info', 4: 'title', 5: 'transport', 6: 'transport_type', 7: 'route'}
            table = table_map.get(table_index, 'employees')
            command = SelectAllCommand(connection.connection, table)
        elif user_choice == 2:
            command = TransportCountCommand(connection.connection)
        elif user_choice == 3:
            command = TransportTypeCountCommand(connection.connection)
        elif user_choice == 10:
            percentage = InputHandler.get_percentage()
            command = UpdateSalaryCommand(connection.connection, percentage)
        elif user_choice == 14:
            table_index = InputHandler.get_table_choice()
            table_map = {1: 'employees', 2: 'bort_squad', 3: 'route_geo_info', 4: 'title', 5: 'transport', 6: 'transport_type', 7: 'route'}
            table = table_map.get(table_index, 'employees')
            order_choice = InputHandler.get_sort_order()
            order = 'ASC' if order_choice == 1 else 'DESC'
            column = input("Введіть назву колонки для сортування: ")
            command = SortCommand(connection.connection, table, column, order)

        if command:
            command.execute()

    connection.disconnect()

if __name__ == '__main__':
    main()
