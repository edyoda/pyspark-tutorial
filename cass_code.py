from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

session.set_keyspace('office')

session.execute('CREATE TABLE user (id int PRIMARY KEY, location text)')
session.execute("INSERT INTO user (id, location) VALUES (%s, %s)",(11,'abc'))



