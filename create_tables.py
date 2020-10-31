import cassandra

from cassandra.cluster import Cluster
from queries import drop_keyspace, drop_table_queries, create_table_queries, create_keyspace


def create_database():
    """
    - Creates and connects to the sparkify
    - Returns the Cassandra cluster and session
    """
    
    # connect to Cassandra
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
    
    # create sparkify database
    session.execute(drop_keyspace)
    session.execute(create_keyspace)
    
    # set session to sparkify keyspace
    session.set_keyspace('sparkify')
    
    return cluster, session


def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Table Drop Error for {}!".format(query))
            print(e)

def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Table Create Error for {}!".format(query))
            print(e)


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    session to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    try:
        cluster, session = create_database()
    except Exception as e:
        print(e)
        raise e
    
    try:
        drop_tables(session)
        create_tables(session)
    except Exception as e:
        print(e)
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()