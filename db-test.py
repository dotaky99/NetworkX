import pandas as pd
import csv
import sqlite3
from itertools import count
import os
# src_ip, dst_ip, src_name, dst_name, country, behavior, incident_type, first_connected, last_connect, info_src(출처), created_at
# edge: src_ip, dst_ip, behavior, first_connected, last_connected, from, created_at
# node: src_name, dst_name, country
# ipname: ip, name 중복 X

# CSV to DB
def db_create():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(this_dir, "data", "NetworkV.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS edge("
                "src_ip TEXT, dst_ip TEXT, behavior TEXT, first_connected TEXT, last_connected TEXT, info_src TEXT, constraint edge primary key(src_ip, dst_ip))")
    cur.execute("CREATE TABLE IF NOT EXISTS node(ip TEXT PRIMARY KEY, incident_type TEXT, country TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS ipname(ip TEXT PRIMARY KEY, name TEXT)")
    conn.commit()

    # file_path = str(input("csv 파일을 드래그 앤 드롭 해주세요.\n"))
    # f = open(file_path, 'r', encoding='UTF8')
    f = open('input_data/input_data2.csv', 'r', encoding='UTF8')
    csv_reader = csv.reader(f)

    print(f'DB에 데이터를 추가합니다.')
    # 0=to_vic, 1=from_att, 2=name_vic, 3=name_att, 4=incident_type
    #   0       1       2           3       4       5               6               7               8           9
    # src_ip, dst_ip, src_name, dst_name, country, behavior, incident_type, first_connected, last_connect, info_src(출처), created_at

    ip_list = [[]]  # to_vic, name_vic, from_att, name_att, incident_type
    for i, row in zip(count(step=2), csv_reader):
        ip_list.append([])
        ip_list[i].append(row[0]) # src_vic
        ip_list[i].append(row[2]) # src_name
        ip_list[i].append(row[4]) # country
        ip_list[i].append(row[6]) # incident_type
        ip_list.append([])
        ip_list[i + 1].append(row[1]) # dst_ip
        ip_list[i + 1].append(row[3]) # dst_name
        ip_list[i + 1].append(row[4]) # country
        ip_list[i + 1].append(row[6]) # incident_type

        # [[src_ip, src_name, behavior, incident_type, first_connected, last_connect, info_src]
        #  [dst_ip, dst_name, behavior, incident_type, first_connected, last_connect, info_src]]
        #     1         2       3           4               5               6           7

        #   0       1       2           3       4       5               6               7               8           9
        # src_ip, dst_ip, src_name, dst_name, country, behavior, incident_type, first_connected, last_connect, info_src(출처), created_at
        edge_sql = f'INSERT INTO edge (src_ip, dst_ip, behavior, first_connected, last_connected, info_src) VALUES (?, ?, ?, ?, ?, ?)'
        # edge_sql = f'INSERT INTO edge (src_ip, dst_ip, behavior, first_connected, last_connected, info_src) ' \
        #            f'VALUES (?, ?, ?, ?, ?, ?)' \
        #            f'WHERE NOT EXISTS (' \
        #            f'SELECT src_ip, dst_ip FROM edge WHERE src_ip = ? AND dst_ip = ?)'
        # src_ip, dst_ip, behavior, first, last, info_src
        try:
            cur.execute(edge_sql, (row[0], row[1], row[5], row[7], row[8], row[9]))
        except:
            pass

    for row in ip_list:
        try:
            node_sql = f'INSERT INTO node (ip, incident_type, country) VALUES (?, ?, ?)'
            cur.execute(node_sql, (row[0], row[3], row[2]))
        except:
            pass

        try:
            ip_name_sql = f'INSERT INTO ipname (ip, name) VALUES (?, ?)'
            cur.execute(ip_name_sql, (row[0], row[1]))
        except:
            pass

    conn.commit()
    f.close()
    conn.close()
    print(f'DB 데이터 추가 작업이 완료 됐습니다.')

# DB to CSV for Parse Data
def csv_create():
    print(f'데이터 파싱을 위한 DB연결을 시작합니다.')
    this_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(this_dir, "data", "NetworkV.db")
    conn = sqlite3.connect(db_path)
    edge_query = 'SELECT B.name as "from", C.name as "to", A.behavior, A.info_src FROM edge A ' \
                 'INNER JOIN ipname B ON A.src_ip = B.ip ' \
                 'INNER JOIN ipname C ON A.dst_ip = C.ip'

    node_query = 'SELECT B.name as "id", A.incident_type, A.country FROM node A ' \
                 'INNER JOIN ipname B ON A.ip = B.ip'
    edge_df = pd.read_sql(edge_query, conn)
    node_df = pd.read_sql(node_query, conn)
    edge_df.to_csv(os.path.join(this_dir, "data", r'edge.csv'), index=False)
    node_df.to_csv(os.path.join(this_dir, "data", r'node.csv'), index=False)
    print(f'데이터 파싱이 완료 됐습니다.')

    conn.close()


if __name__ == '__main__':

    db_create()
    csv_create()
    print(f'\n\n Done.')
