import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_("created"), auto_now_add=True)
    modified = models.DateTimeField(_("modified"), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.TextField(_("name"), unique=True)
    description = models.TextField(_("description"), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _("Genre")
        verbose_name_plural = _("Genres")


class Person(UUIDMixin, TimeStampedMixin):

    full_name = models.TextField(_("Full name"), db_index=True)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _("Person")
        verbose_name_plural = _("Person")


class FilmWork(UUIDMixin, TimeStampedMixin):
    class FilmWorkType(models.TextChoices):
        MOVIE = "movie", _("Movie")
        TV_SHOW = "tv_show", _("TV Show")

    title = models.TextField(_("name"), db_index=True)
    description = models.TextField(_("description"))
    creation_date = models.DateField(_("creation_date"), db_index=True)
    rating = models.FloatField(_("rating"), db_index=True, validators=[MinValueValidator(0), MaxValueValidator(100)])
    type = models.CharField(_("type"), max_length=7, choices=FilmWorkType.choices)
    genres = models.ManyToManyField(Genre, through="GenreFilmWork")
    persons = models.ManyToManyField(Person, through="PersonFilmWork", verbose_name=_("Genres"))

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _("Film work")
        verbose_name_plural = _("Film works")


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE, verbose_name=_("genre"))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        unique_together = ("genre_id", "film_work_id")
        verbose_name = _("Film work genre")
        verbose_name_plural = _("Film works genres")

    def __str__(self):
        return f"_(Genres in film work) {self.film_work}"


class PersonFilmWork(UUIDMixin):
    class RolesList(models.TextChoices):
        DIRECTOR = "director", _("Director")
        WRITER = "writer", _("Writer")
        ACTOR = "actor", _("Actor")
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE, verbose_name=_("person"))
    role = models.CharField(verbose_name=_("Role"), max_length=8, choices=RolesList.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        unique_together = ("person_id", "film_work_id", "role")
        verbose_name = _("Person in film work")
        verbose_name_plural = _("Persons in film work")

    def __str__(self):
        return f"_(Persons in film work) {self.film_work}"
