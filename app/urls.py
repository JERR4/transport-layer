from django.urls import path

from .views import *

urlpatterns = [
    path('transfer/', transfer),
    path('send/', send)
]