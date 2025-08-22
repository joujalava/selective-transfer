import argparse

DATABASE_URL = "postgresql://username:password@localhost:5432/"

INITIAL_DATABASE_TABLE = "A".lower()

INITIAL_COLUMN = "name"

PARSER = argparse.ArgumentParser()

PARSER.add_argument("-d", "--destination", help="name of destination database")

PARSER.add_argument("-s", "--source", help="name of source database")

PARSER.add_argument("-i", "--ids", help="ids of rows to copy from source database")