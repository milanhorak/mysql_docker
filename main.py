#!/usr/bin/env python3
"""
steps done before to set up my SQL in Docker:

download mysql image
docker pull mysql

check if pulled right
docker image ls

start docker container
docker run --name=mysql_docker --env="MYSQL_ROOT_PASSWORD=root_password" -p 3306:3306 -d mysql

check if mysql docker is running properly
docker ps

access mysql from terminal
docker exec -it mysql_docker mysql -uroot -proot_password

create a new user
CREATE USER 'milan'@'%' IDENTIFIED BY '1234';
GRANT ALL PRIVILEGES ON mysql_docker_db.* to 'milan'@'%';
exit
"""

import csv
import time
import operator
import mysql.connector as connector  # Database connection
from mysql.connector import errorcode  # Error codes
from collections import Counter


def read_csv(file: csv) -> list[list[str]]:
    # read csv and return list
    try:
        with open(file, "r") as f:
            reader = csv.reader(f)
            # cleans empty rows
            my_list = [row for row in reader if row]
            f.close()
        return my_list
    except FileNotFoundError as err:
        exit(err)


def print_list(my_list):
    # prints list
    for row in my_list:
        print(row)


def time_list(my_list):
    # adds 'clickTimestamp_sec'
    header = my_list.pop(0)
    header.append('clickTimestamp_sec')
    my_list = [[row[0], row[1], time.mktime(time.strptime(row[0], "%Y-%m-%d %H:%M:%S"))] for row in my_list]
    my_list.insert(0, header)
    return my_list


def sort_list(my_list, args):
    """
    Sort a list by multiple attributes

    inspired by: https://stackoverflow.com/questions/4233476/sort-a-list-by-multiple-attributes?rq=1

    have a list of lists
    [[12, 'tall', 'blue', 1],
    [2, 'short', 'red', 9],
    [4, 'tall', 'blue', 13]]

    A key can be a function that returns a tuple:

    s = sorted(s, key=lambda x: (x[1], x[2]))

    Or you can achieve the same using itemgetter(which is faster and avoids a Python function call):

    import operator
    s = sorted(s, key=operator.itemgetter(1, 2))

    And notice that here you can use sort instead of using sorted and then reassigning:

    s.sort(key=operator.itemgetter(1, 2))
    """

    header_my_list = my_list.pop(0)
    my_list.sort(key=operator.itemgetter(*args))
    my_list.insert(0, header_my_list)
    return my_list


def defraud(clicks, impressions):
    # defraud clicks

    # from clicks remove header and extend
    header_clicks = clicks.pop(0)
    header_clicks.extend(['adId', 'visitorHash'])

    # dic from impressions
    impressions_dic = {f"{row[1]}": (row[2], row[3]) for row in impressions[1:]}

    # prepare list 4 defraud
    # 'clickTimestamp', 'impressionId', 'clickTimestamp_sec', adID, visitorHash

    defraud_li = [
        [row[0], row[1], row[2], impressions_dic.get(row[1])[0], impressions_dic.get(row[1])[1]] for row in clicks
    ]
    defraud_li.insert(0, header_clicks)

    # sort by adId, visitorHash, clickTimestamp_sec
    # ['clickTimestamp', 'impressionId', 'clickTimestamp_sec', 'adId', 'visitorHash']
    args = 3, 4, 2
    defraud_li = sort_list(defraud_li, args)

    # defraud clicks
    new_li = []
    pom = defraud_li[0]
    for row in defraud_li[1:]:
        if (row[3] != pom[3] or row[4] != pom[4]) or (row[2] - pom[2]) > 600:
            new_li.append(row)
        pom = row
    new_li.insert(0, header_clicks)
    return new_li


def get_result(clicks, impressions):
    # prepares data to be saved into mysql
    # date, adId, clicks_count, impressions_count

    # prepare data from clicks

    # transform data from clicks
    clicks = [row[0][:10] + "=" + row[3] for row in clicks[1:]]
    # count num of items per item as dictionary
    c = Counter(clicks)
    clicks_dic = {f"{item}": c[item] for item in clicks}

    # prepare data from impressions

    # transform data from impressions
    impressions = [row[0][:10] + "=" + row[2] for row in impressions[1:]]
    # count num of items per item as dictionary
    c = Counter(impressions)
    impressions_dic = {f"{item}": c[item] for item in impressions}

    # unique date=adId
    my_set = set(clicks + impressions)

    # result
    # resultId, date, adId, clicks_count, impressions_count
    result = [
        (f"{item}", f"{item}"[:10], f"{item}"[11:], clicks_dic.get(item), impressions_dic.get(item)) for item in my_set
    ]

    return result


def create_database(conn, db_name):
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8'")
        cur.execute(f"USE {db_name}")
    except connector.Error as err:
        print("Database error:", err)


def create_connection(config, db_name):
    conn = None
    try:
        conn = connector.connect(**config)
        cur = conn.cursor()
        cur.execute(f"USE {db_name}")
        return conn
    except connector.Error as err:
        if err.errno == errorcode.ER_BAD_HOST_ERROR:
            print("Wrong DB server address.", err)
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Apparently the username or password is incorrect.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database: {db_name} does not exist.")
            create_database(conn, db_name)
            print(f"Database: {db_name} created.")
            return conn
        exit(err)


def create_result_table(conn, table):
    try:
        cur = conn.cursor()
        cur.execute(table)
        conn.commit()
    except connector.Error as err:
        print(err)


def result_table(conn, insert_sql, ins_data, table, tb_name):
    # write data into table
    mod = True
    while mod:
        try:
            cur = conn.cursor()
            cur.executemany(insert_sql, ins_data)
            conn.commit()
            mod = False
            print(f"Data writen into {tb_name}.")

        except connector.Error as err:
            # table not exist
            if err.errno == errorcode.ER_NO_SUCH_TABLE:
                print(f"Table: {tb_name} does not exist.")
                create_result_table(conn, table)
                print(f"Table: {tb_name}  created.")

            # data duplicate
            elif err.errno == errorcode.ER_DUP_ENTRY:
                print(err)
                print("Write to table not done.")
                mod = False
            else:
                print(err)
                mod = False


def execute_query(conn, ins_sql):
    cur = conn.cursor()
    cur.execute(ins_sql)
    rows = cur.fetchall()
    print()
    print(ins_sql)
    print("Number of rows: ", cur.rowcount)
    print(cur.column_names)
    print_list(rows)


def main():
    cliks_file = 'clicks.csv'
    impressions_file = 'impressions.csv'

    clicks: list[list[str]] = read_csv(cliks_file)  # assigns a cleaned csv file to variable
    # print("\n after read cliks.csv")
    # print_list(clicks)
    # print()

    clicks = time_list(clicks)  # add clickTimestamp_sec from clickTimestamp
    # print("\n after time")
    # print_list(clicks)
    # print()

    impressions = read_csv(impressions_file)  # assigns a cleaned csv file to variable
    # print("\n after read impressions.csv ")
    # print_list(impressions)
    # print()

    clicks = defraud(clicks, impressions)  # defraud clicks
    # print("\n after defraud")
    # print_list(clicks)
    # print()

    result = get_result(clicks, impressions)  # prepare data to write into mysql
    # print("\n result")
    # print("(resultId, date, adId, clicks_count, impressions_count)")
    # print_list(result)
    # print()

    # make connection
    config = {
        "user": "milan",
        "password": "1234",
        'port': 3306,
        "host": "localhost"
    }
    db_name = "mysql_docker_db"
    conn = create_connection(config, db_name)

    # insert data into a table if it does not exist so create it
    tb_name = "result_tb"

    ins_sql = f"""
    INSERT INTO {tb_name} (resultId, date, adId, clicksCount, impressionsCount) VALUES (%s, %s, %s, %s, %s)
    """

    ins_data = result
    # after 2nd part of vr should be # date, idID, clicksCount, impressionsCount
    table = f"""
    CREATE TABLE {tb_name} (
    resultId VARCHAR(255) NOT NULL PRIMARY KEY,
    date VARCHAR(10) NOT NULL,
    adId INT NOT NULL,
    clicksCount INT,
    impressionsCount INT
    )"""

    result_table(conn, ins_sql, ins_data, table, tb_name)  # write data into table

    # print query
    ins_sql = "SELECT * FROM result_tb ORDER by date, adId"
    execute_query(conn, ins_sql)

    # # how many impressions the ad with ID X had on day Y?
    ins_sql = "SELECT date, adId, impressionsCount FROM result_tb WHERE date = '2021-04-21' AND adID = 1"
    execute_query(conn, ins_sql)

    # # the total number of clicks in the system per day Y?
    ins_sql = "SELECT SUM(clicksCount) AS 'SUM_clicks' FROM result_tb WHERE date = '2021-04-21'"
    execute_query(conn, ins_sql)

    ins_sql = "SELECT date, clicksCount FROM result_tb WHERE date = '2021-04-21'"
    execute_query(conn, ins_sql)

    conn.close()


if __name__ == '__main__':
    start_time = time.process_time()
    main()
    print()
    print("\nExecution time: ", time.process_time() - start_time, "sec")
