from flask import Flask, request, jsonify
import sqlite3
from sqlite3 import Error
import requests
import json

app = Flask(__name__)
db_path = "books.db"


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path, check_same_thread=False)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


connection = create_connection(db_path)


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
        return True
    except Error as e:
        print(f"The error '{e}' occurred")
        return False


def execute_read_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        print("Query executed successfully")
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


def row_exists():
    row_exists = execute_read_query(connection, "SELECT * FROM books_data")
    if row_exists:
        return True
    else:
        return False


def get_all_books_data(book_id=""):
    params = {"q": "Hobbit"}
    response = requests.get("https://www.googleapis.com/books/v1/volumes", params=params).json()
    books_data = response["items"]
    all_books = books_with_matching_id = []
    for i in books_data:
        vol_info = i["volumeInfo"]
        title = vol_info["title"]
        authors = vol_info["authors"]
        published_date = vol_info["publishedDate"]
        categories = average_rating = rating_counts = thumbnail = "None"
        # these keys are sometimes missing
        if "categories" in vol_info:
            categories = vol_info["categories"]
        if "averageRating" in vol_info:
            average_rating = vol_info["averageRating"]
        if "ratingsCount" in vol_info:
            rating_counts = vol_info["ratingsCount"]
        if "imageLinks" in vol_info:
            thumbnail = vol_info["imageLinks"]["thumbnail"]
        book = {"title": title, "authors": authors, "published_date": published_date, "categories": categories,
                "average_rating": average_rating, "rating_counts": rating_counts, "thumbnail": thumbnail}
        if book_id:
            if book_id == i["id"]:
                books_with_matching_id.append(book)
        else:
            all_books.append(book)
    if book_id:
        return books_with_matching_id
    else:
        return all_books


@app.route("/books")
def get_books():
    all_books = get_all_books_data()
    if request.args.get("published_date"):
        published_date = request.args.get("published_date")
        books_with_matching_date = [book for book in all_books if published_date in book["published_date"]]
        if len(books_with_matching_date) > 0:
            return jsonify(books_with_matching_date)
        else:
            return {"message": "No books matching your search"}
    elif request.args.get("sort"):
        sort_key = request.args.get("sort")
        if sort_key in all_books[0]:
            sorted_books = sorted(all_books, key=lambda x: str(x[sort_key]))
            return jsonify(sorted_books)
        else:
            return {"message": "You passed wrong key"}
    elif request.args.get("author"):
        authors = request.args.getlist("author")
        all_authors_books = []
        for author in authors:
            author_books = [book for book in all_books if author in book["authors"]]
            if len(author_books) > 0:
                all_authors_books.append(author_books)
        if len(all_authors_books) > 0:
            return jsonify(all_authors_books)
        else:
            return {"message": "No books matching your search"}
    else:
        return jsonify(all_books)


@app.route("/books/<book_id>")
def get_book_by_id(book_id):
    books = get_all_books_data(book_id)
    if len(books) > 0:
        return jsonify(books)
    else:
        return {"message": "No books with this id"}


@app.route("/db")
def download_books_data():
    params = {"q": "war"}
    response = requests.get("https://www.googleapis.com/books/v1/volumes", params=params).json()
    books_json = json.dumps(response)
    books_json_clean = books_json.replace("'", "**")
    # ' causing syntax error
    if row_exists():
        query = f"UPDATE books_data SET json_data='{books_json_clean}' WHERE id=1"
    else:
        query = f"INSERT INTO books_data(json_data) VALUES('{books_json_clean}')"
    if execute_query(connection, query):
        return {"message": "Data successfully saved"}
    else:
        return {"message": "Error while saving data"}


if __name__ == "__main__":
    app.run(debug=True)