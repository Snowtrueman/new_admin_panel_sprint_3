import math

from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class NumberPaginationNoLinks(PageNumberPagination):
    """
    Pagination class for custom params in response
    """

    page_size = 50
    max_page_size = 50
    page_query_param = "page"

    def get_paginated_response(self, data):
        return Response({
            "count": self.page.paginator.count,
            "total_pages": math.ceil(self.page.paginator.count / self.page_size),
            "prev": self.page.previous_page_number() if self.page.has_previous() else None,
            "next": self.page.next_page_number() if self.page.has_next() else None,
            "results": data
        })
