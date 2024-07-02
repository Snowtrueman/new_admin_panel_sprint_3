from django.urls import path

from . import views


urlpatterns = [
    path('movies/', views.MoviesListApiView.as_view()),
    path('movies/<uuid:pk>/', views.MoviesDetailApiView.as_view()),
]
