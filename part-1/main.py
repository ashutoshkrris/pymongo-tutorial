from bson.objectid import ObjectId
from decouple import config
from pymongo import MongoClient

# Fetching environment variables
USERNAME = config('MONGODB_USERNAME')
PASSWORD = config('MONGODB_PASSWORD')

# print(USERNAME, PASSWORD)

connection_string = f"mongodb+srv://{USERNAME}:{PASSWORD}@cluster0.cglieum.mongodb.net/?retryWrites=true&w=majority"

# Creating a connection to MongoDB
client = MongoClient(connection_string)


# Creating a new database
library_db = client.library_db

# Creating a new collection
book_collection = library_db.books

# databases = client.list_database_names()

print(databases)

# Inserting new document
book_data = {
    "author": "Chinua Achebe",
    "country": "Nigeria",
    "imageLink": "images/things-fall-apart.jpg",
    "language": "English",
    "link": "https://en.wikipedia.org/wiki/Things_Fall_Apart\n",
    "pages": 209,
    "title": "Things Fall Apart",
    "year": 1958
}

book = book_collection.insert_one(book_data)
print(book.inserted_id)

books_data = [
    {
        "author": "Hans Christian Andersen",
        "country": "Denmark",
        "imageLink": "images/fairy-tales.jpg",
        "language": "Danish",
        "link": "https://en.wikipedia.org/wiki/Fairy_Tales_Told_for_Children._First_Collection.\n",
        "pages": 784,
        "title": "Fairy tales",
        "year": 1836
    },
    {
        "author": "Dante Alighieri",
        "country": "Italy",
        "imageLink": "images/the-divine-comedy.jpg",
        "language": "Italian",
        "link": "https://en.wikipedia.org/wiki/Divine_Comedy\n",
        "pages": 928,
        "title": "The Divine Comedy",
        "year": 1315
    },
    {
        "author": "Unknown",
        "country": "Sumer and Akkadian Empire",
        "imageLink": "images/the-epic-of-gilgamesh.jpg",
        "language": "Akkadian",
        "link": "https://en.wikipedia.org/wiki/Epic_of_Gilgamesh\n",
        "pages": 160,
        "title": "The Epic Of Gilgamesh",
        "year": -1700
    },
    {
        "author": "Unknown",
        "country": "Achaemenid Empire",
        "imageLink": "images/the-book-of-job.jpg",
        "language": "Hebrew",
        "link": "https://en.wikipedia.org/wiki/Book_of_Job\n",
        "pages": 176,
        "title": "The Book Of Job",
        "year": -600
    }
]

books = book_collection.insert_many(books_data)
print(books.inserted_ids)

# # Reading documents

books = book_collection.find()
for book in books:
    print(book)

book = book_collection.find_one(filter={"author": "Dante Alighieri"})
print(book)

from bson.objectid import ObjectId

book_id = ObjectId("63934049d13c8fbef5a3bfe5")
book = book_collection.find_one(filter={"_id": book_id})
print(book)

projection = ["author", "language", "title"]
books = book_collection.find(filter={}, projection=projection)
print(list(books))

total_books = book_collection.count_documents(filter={})
print(total_books)

# Updating documents
book_id = ObjectId("63934049d13c8fbef5a3bfe5")
updates = {
    "$set": {
        "author": "Unknown Author"
    }
}
book_collection.update_one(filter={"_id": book_id}, update=updates)
book_collection.update_many(filter={}, update={"$set": {"author": "Unknown Author"}})

# Deleting documents
from bson.objectid import ObjectId
book_id = ObjectId("6383ba5c7c58adf893a71694")
book_collection.delete_one(filter={"_id": book_id})
book_collection.delete_many(filter={})
