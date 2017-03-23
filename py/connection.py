import mysql.connector

class Connection:
    """Handles basic single MySQL connection instance"""
    def __init__(self):
        self.__cnx = mysql.connector.connect(user='root', host='127.0.0.1', database='merge')
        self.__cnx.reconnect(attempts=100, delay=5)

    def getconn(self):
        """Returns the MySQL connection instance"""
        return self.__cnx

    def closeconn(self):
        """Closes the MySQL connection instance"""
        self.__cnx.close()
