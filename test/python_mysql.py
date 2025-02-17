import mysql.connector


HOST = "localhost"
USER = "santi"
PASSWORD = "Mysql_Pssw2025!"
DATABASE_NAME = "noise_port_test"



def test_db():
    db = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE_NAME,
        allow_local_infile=True  # equivalent to --local-infile=1 in the CLI
    )

    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    print(tables)

    
    # close
    cursor.close()
    db.close()



def main():
    test_db()

if __name__ == "__main__":
    main()
