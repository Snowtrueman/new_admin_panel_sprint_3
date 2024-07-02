from rest_framework import serializers

from movies.models import FilmWork


class FilmWorkSerializer(serializers.ModelSerializer):
    """
    Serializer for list and detail film work serialization
    """

    genres = serializers.ListField(source="filmwork_genres")
    actors = serializers.ListField(source="filmwork_actors")
    directors = serializers.ListField(source="filmwork_directors")
    writers = serializers.ListField(source="filmwork_writers")

    class Meta:
        model = FilmWork
        fields = (
            "id",
            "title",
            "description",
            "creation_date",
            "rating",
            "type",
            "genres",
            "actors",
            "directors",
            "writers"
        )
