from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('university')

session.execute("CREATE TYPE address (street text, zipcode int)")
session.execute("CREATE TABLE user (id int PRIMARY KEY, location frozen<address>)")



# create a class to map to the "address" UDT
class Address(object):

    def __init__(self, street, zipcode):
        self.street = street
        self.zipcode = zipcode

#cluster.register_user_type('university', 'address', Address)

data = [Address("123 Main St.", 78723), Address("123 Main St.", 78723)]

# insert a row using an instance of Address
for idx,d in ennumerate(data):
    session.execute("INSERT INTO user (id, location) VALUES (%s, %s)",
                (idx, d))

# results will include Address instances
results = session.execute("SELECT * FROM user")
row = results.one()
print (row.id, row.location.street, row.location.zipcode)
