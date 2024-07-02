import uuid
import datetime
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Genre:
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Person:
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class FilmWork:
    title: str
    description: str
    type: Literal["movie", "tv_show"]
    created: datetime.datetime
    modified: datetime.datetime
    creation_date: datetime.date
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self):
        if self.creation_date is None or self.creation_date == "":
            self.creation_date = datetime.date(2000, 1, 1)
        if self.rating is None or self.rating == "":
            self.rating = 0.0


@dataclass
class GenreFilmWork:
    genre_id: uuid.UUID
    film_work_id: uuid.UUID
    created: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    person_id: uuid.UUID
    film_work_id: uuid.UUID
    role: str
    created: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
