import mysql.connector

def create_table():
    try:
        db_conn = mysql.connector.connect(host='localhost',user='root',database = 'mytestdb2',password='root123')
        cursor = db_conn.cursor()
        sql = "CREATE TABLE libgen_db (topic_id INT,page INT,id INT,authors JSON,file_name VARCHAR(500),file_link varchar(500),file_edition varchar(100),isin JSON,publisher VARCHAR(255),publish_year VARCHAR(255),pages VARCHAR(255), lang VARCHAR(255), file_size VARCHAR(255), file_extension VARCHAR(100), mirror_links json, page_url VARCHAR(255), PRIMARY KEY(id));"
        cursor.execute(sql)
        db_conn.commit()
    except Exception as e:
        print ("Error while connecting to MySQL using Connection pool ", e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

create_table()