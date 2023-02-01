from datetime import datetime
from pprint import pprint
from bson import ObjectId

from decouple import config
from pymongo import MongoClient

# Fetching environment variables
USERNAME = config('MONGODB_USERNAME')
PASSWORD = config('MONGODB_PASSWORD')

connection_string = f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.d0mjzrc.mongodb.net/?retryWrites=true&w=majority&authSource=admin"

# Creating a connection to MongoDB
client = MongoClient(connection_string)


# Creating a new database
library_db = client.library_db


def create_book_collection():

    # Creating a new collection
    try:
        library_db.create_collection("book")
    except Exception as e:
        print(e)

    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publication_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "description": "must be an array and is required",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be an objectId and is required"
                    },
                    "minItems": 1,
                },
                "publication_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "type": {
                    "enum": ["hardcover", "paperback"],
                    "description": "can only be one of the enum values and is required"
                },
                "copies": {
                    "bsonType": "int",
                    "description": "must be an integer greater than 0 and is required",
                    "minimum": 0
                }
            }
        }
    }

    library_db.command("collMod", "book", validator=book_validator)

create_book_collection()


def create_author_collection():
    try:
        library_db.create_collection("author")
    except Exception as e:
        print(e)

    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date"
                }
            }
        }
    }

    library_db.command("collMod", "author", validator=author_validator)


create_author_collection()

# Listing collections' validations
print(f'Book Validation: {library_db.get_collection("book").options()}')
print(f'Author Validation: {library_db.get_collection("author").options()}')

def insert_bulk_data():
    authors = [
        {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": datetime(1990, 1, 20)
        },
        {
            "first_name": "Jane",
            "last_name": "Doe",
            "date_of_birth": datetime(1990, 1, 1)
        },
        {
            "first_name": "Jack",
            "last_name": "Smith",
        }
    ]

    author_collection = library_db.author
    author_ids = author_collection.insert_many(authors).inserted_ids
    print(f"Author IDs: {author_ids}")

    books = [
        {
            "title": "MongoDB, The Book for Beginners",
            "authors": [author_ids[0], author_ids[1]],
            "publication_date": datetime(2022, 12, 17),
            "type": "hardcover",
            "copies": 10
        },
        {
            "title": "MongoDB, The Book for Advanced Users",
            "authors": [author_ids[0], author_ids[2]],
            "publication_date": datetime(2023, 1, 2),
            "type": "paperback",
            "copies": 5
        },
        {
            "title": "MongoDB, The Book for Experts",
            "authors": [author_ids[1], author_ids[2]],
            "publication_date": datetime(2023, 1, 2),
            "type": "paperback",
            "copies": 5
        },
        {
            "title": "100 Projects in Python",
            "authors": [author_ids[0]],
            "publication_date": datetime(2022, 1, 2),
            "type": "hardcover",
            "copies": 20
        },
        {
            "title": "100 Projects in JavaScript",
            "authors": [author_ids[1]],
            "publication_date": datetime(2022, 1, 2),
            "type": "paperback",
            "copies": 15
        }
    ]

    book_collection = library_db.book
    inserted_book_ids = book_collection.insert_many(books)

    print(f"Inserted Book Results: {inserted_book_ids}")
    print(f"Inserted Book IDs: {inserted_book_ids.inserted_ids}")

insert_bulk_data()


book_collection = library_db.book
book_collection.insert_one({
    "title": "MongoDB, The Book"
})


# Using Regex
query = {"title": {"$regex": "MongoDB"}}
mongodb_books = library_db.book.find(query)
pprint(list(mongodb_books))

# Find all authors with its books

pipeline = [
    {"$lookup": {
        "from": "book",            "localField": "_id",
        "foreignField": "authors",
        "as": "books"
        }
    }
]
authors_with_books = library_db.author.aggregate(pipeline)
pprint(list(authors_with_books))

# Get count of books

pipeline = [
    {
        "$lookup": {
            "from": "book",            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    },
    {
        "$addFields": {
            "total_books": {"$size": "$books"}
        }
    },
    {
        "$project": {"_id": 0, "first_name": 1, "last_name": 1, "total_books": 1}
    }
]
authors_with_books = library_db.author.aggregate(pipeline)
pprint(list(authors_with_books))


# Get authors with more than 2 books
pipeline = [
    {
        "$lookup": {
            "from": "book",
            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    },
    {
        "$addFields": {
            "total_books": {"$size": "$books"}
        }
    },
    {
        "$match": {"total_books": {"$gt": 2}}
    },
    {
        "$project": {"_id": 0, "first_name": 1, "last_name": 1, "total_books": 1}
    }
]
authors_with_more_than_2_books = library_db.author.aggregate(pipeline)
pprint(list(authors_with_more_than_2_books))


# Calculate age of books
# Define the pipeline
pipeline = [
    {
        "$lookup": {
            "from": "book",
            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    },
    {
        "$addFields": {
            "books": {
                "$map": {
                    "input": "$books",
                    "as": "book",
                    "in": {
                        "title": "$$book.title",
                        "age_in_days": {
                            "$dateDiff": {
                                "startDate": "$$book.publication_date",
                                "endDate": "$$NOW",
                                "unit": "day"
                            }
                        }
                    }
                }
            }
        }
    }
]

# Execute the pipeline
authors_with_book_ages = library_db.author.aggregate(pipeline)
pprint(list(authors_with_book_ages))

