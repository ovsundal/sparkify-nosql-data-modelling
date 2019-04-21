from cassandra.cluster import Cluster
from sql_statements import create_keyspace_sparkifydb, create_tables, drop_tables


cluster = Cluster()

session = cluster.connect()

# create keyspace
try:
    session.execute(create_keyspace_sparkifydb)
    session.set_keyspace('sparkifydb')
except Exception as e:
    print('Error creating/setting keyspace')
    print(e)

# create tables
try:
    for create_table in create_tables:
        session.execute(create_table)
except Exception as e:
    print('Error creating tables')
    print(e)

# drop tables
# try:
#     for drop_table in drop_tables:
#         session.execute(drop_table)
# except Exception as e:
#     print('Error dropping tables')
#     print(e)

