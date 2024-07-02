from django.contrib import admin

from .models import Genre, FilmWork, Person, GenreFilmWork, PersonFilmWork


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    extra = 1
    autocomplete_fields = ("genre",)


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    extra = 1
    autocomplete_fields = ("person",)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("name", "description",)
    search_fields = ("name",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("full_name",)
    search_fields = ("full_name",)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    list_display = ("title", "description", "creation_date", "rating", "type",)
    list_filter = ("type",)
    search_fields = ("title",)
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related("genres", "persons")
