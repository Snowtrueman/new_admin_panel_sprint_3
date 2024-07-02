import os
from dotenv import load_dotenv

load_dotenv()

TABLES_TO_EXPORT = ("genre", "person", "film_work", "genre_film_work", "person_film_work")

EXPORT_TABLE_FIELDS_MAPPER = {
    "genre": ("id", "name", "description", "created_at", "updated_at"),
    "person": ("id", "full_name", "created_at", "updated_at"),
    "film_work": ("id", "title", "description", "creation_date", "rating", "type", "created_at", "updated_at"),
    "genre_film_work": ("id", "film_work_id", "genre_id", "created_at"),
    "person_film_work": ("id", "film_work_id", "person_id", "role", "created_at")
}

IMPORT_TABLE_FIELDS_MAPPER = {
    "genre": {"id": "id", "name": "name", "description": "description", "created_at": "created",
              "updated_at": "modified"},
    "person": {"id": "id", "full_name": "full_name", "created_at": "created", "updated_at": "modified"},
    "film_work": {"id": "id", "title": "title", "description": "description", "creation_date": "creation_date",
                  "rating": "rating", "type": "type", "created_at": "created", "updated_at": "modified"},
    "genre_film_work": {"id": "id", "film_work_id": "film_work_id", "genre_id": "genre_id", "created_at": "created"},
    "person_film_work": {"id": "id", "film_work_id": "film_work_id", "person_id": "person_id", "role": "role",
                         "created_at": "created"}
}

CHUNK_SIZE = 20

POSTGRES_DSN = {
    "dbname": os.environ.get("POSTGRES_DBNAME", "postgres"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": os.environ.get("POSTGRES_PORT", 5432)}

POSTGRES_SCHEMA = "content"

SQLITE_FILE = "db.sqlite"

EXPORT_FILES_DIR = "csv_files"

EXPORT_TABLE_DATACLASS_MAPPER = {
    "genre": "Genre",
    "person": "Person",
    "film_work": "FilmWork",
    "genre_film_work": "GenreFilmWork",
    "person_film_work": "PersonFilmWork"
}
