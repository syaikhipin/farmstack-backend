import logging
from core.utils import (
    DefaultPagination,
    CustomPagination,
    Utils,
    csv_and_xlsx_file_validatation,
    date_formater,
    read_contents_from_csv_or_xlsx_file,
)
from django.db.models import Q
from django.shortcuts import render
from accounts.models import User, UserRole
from core.constants import Constants
from datahub.models import Organization, Datasets, UserOrganizationMap
from microsite.serializers import OrganizationMicrositeSerializer, DatasetsMicrositeSerializer
from rest_framework import pagination, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

LOGGER = logging.getLogger(__name__)


class OrganizationMicrositeViewSet(GenericViewSet):
    """Organization viewset for microsite"""

    permission_classes = []

    @action(detail=False, methods=["get"])
    def admin_organization(self, request):
        """GET method: retrieve an object of Organization using User ID of the User (IMPORTANT: Using USER ID instead of Organization ID)"""
        user_obj = User.objects.filter(role_id=1).first()
        user_org_queryset = UserOrganizationMap.objects.prefetch_related(
            Constants.USER, Constants.ORGANIZATION
        ).filter(user=user_obj.id)

        if not user_org_queryset:
            data = {Constants.ORGANIZATION: None}
            return Response(data, status=status.HTTP_200_OK)

        org_obj = Organization.objects.get(id=user_org_queryset.first().organization_id)
        user_org_serializer = OrganizationMicrositeSerializer(org_obj)
        data = {Constants.ORGANIZATION: user_org_serializer.data}
        return Response(data, status=status.HTTP_200_OK)


class DatasetsMicrositeViewSet(GenericViewSet):
    """Datasets viewset for microsite"""

    serializer_class = DatasetsMicrositeSerializer
    pagination_class = CustomPagination
    permission_classes = []

    def list(self, request):
        """GET method: retrieve a list of dataset objects"""
        dataset = (
            Datasets.objects.select_related(
                Constants.USER_MAP, Constants.USER_MAP_USER, Constants.USER_MAP_ORGANIZATION
            )
            .filter(user_map__user__status=True, status=True)
            .order_by(Constants.UPDATED_AT)
            .all()
        )

        page = self.paginate_queryset(dataset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(self.queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"])
    def dataset_filters(self, request, *args, **kwargs):
        """This function get the filter args in body. based on the filter args orm filters the data."""
        data = request.data
        range = {}
        created_at__range = request.data.pop(Constants.CREATED_AT__RANGE, None)
        if created_at__range:
            range[Constants.CREATED_AT__RANGE] = date_formater(created_at__range)
        try:
            data = Datasets.objects.filter(status=True, **data, **range).order_by(Constants.UPDATED_AT).all()
        except Exception as error:  # type: ignore
            LOGGER.error("Error while filtering the datasets. ERROR: %s", error)
            return Response(f"Invalid filter fields: {list(request.data.keys())}", status=500)

        page = self.paginate_queryset(data)
        serializer = DatasetsMicrositeSerializer(page, many=True)
        return self.get_paginated_response(serializer.data)

    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        data = Datasets.objects.select_related(
            Constants.USER_MAP,
            Constants.USER_MAP_USER,
            Constants.USER_MAP_ORGANIZATION,
        ).filter(
            Q(user_map__user__status=True, status=True, id=pk)
            & (Q(user_map__user__role=1) | Q(user_map__user__role=3))
        )

        serializer = DatasetsMicrositeSerializer(data, many=True)
        if serializer.data:
            data = serializer.data[0]
            data[Constants.CONTENT] = read_contents_from_csv_or_xlsx_file(data.get(Constants.SAMPLE_DATASET))
            return Response(data, status=status.HTTP_200_OK)
        return Response({}, status=status.HTTP_200_OK)
