from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from rest_framework.generics import ListAPIView, RetrieveAPIView

from movies.models import FilmWork
from movies.models import PersonFilmWork
from .serializers import FilmWorkSerializer


class MoviesApiMixin:
    """
    Mixin for dealing with FilmWork model objects
    """

    serializer_class = FilmWorkSerializer
    queryset = (FilmWork.objects.prefetch_related("genres", "persons")
                .annotate(filmwork_genres=ArrayAgg("genres__name", distinct=True))
                .annotate(filmwork_actors=ArrayAgg("persons__full_name", distinct=True,
                                                   filter=Q(personfilmwork__role=PersonFilmWork.RolesList.ACTOR.value)))
                .annotate(filmwork_directors=ArrayAgg("persons__full_name", distinct=True,
                                                      filter=Q(personfilmwork__role=
                                                               PersonFilmWork.RolesList.DIRECTOR.value)))
                .annotate(filmwork_writers=ArrayAgg("persons__full_name", distinct=True,
                                                    filter=Q(personfilmwork__role=
                                                             PersonFilmWork.RolesList.WRITER.value)))
                ).order_by("created")


class MoviesListApiView(MoviesApiMixin, ListAPIView):
    """
    FilmWork model objects list view
    """


class MoviesDetailApiView(MoviesApiMixin, RetrieveAPIView):
    """
    FilmWork model objects detail view
    """
