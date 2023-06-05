import json
import logging
import os
import shutil
from urllib.parse import unquote

import pandas as pd
from django.db import transaction
from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet, ViewSet

from connectors.models import Connectors, ConnectorsMap
from connectors.serializers import (
    ConnectorsCreateSerializer,
    ConnectorsListSerializer,
    ConnectorsMapCreateSerializer,
    ConnectorsMapSerializer,
    ConnectorsRetriveSerializer,
    ConnectorsSerializer,
)
from core import settings
from core.constants import Constants
from core.utils import CustomPagination
from datahub.models import Datasets
from utils.authentication_services import authenticate_user
from utils.jwt_services import http_request_mutation

# Create your views here.


class ConnectorsViewSet(GenericViewSet):
    """Viewset for Product model"""

    queryset = Connectors.objects.all()
    pagination_class = CustomPagination

    @transaction.atomic
    def create(self, request, *args, **kwargs):
        """POST method: create action to save an object by sending a POST request"""
        # setattr(request.data, "_mutable", True)
        data = request.data
        temp_path = f"{settings.TEMP_CONNECTOR_URL}{data.get(Constants.NAME)}.csv"
        dest_path = f"{settings.CONNECTOR_FILES_URL}{data.get(Constants.NAME)}.csv"
        data.pop(Constants.INTEGRATED_FILE)
        if os.path.exists(temp_path):
            shutil.move(temp_path, dest_path)
        serializer = ConnectorsCreateSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        Connectors.objects.filter(id=serializer.data.get(Constants.ID)
                                  ).update(integrated_file=dest_path)
        connectors_data = serializer.data
        for maps in request.data.get(Constants.MAPS, []):
            maps[Constants.CONNECTORS] = connectors_data.get(Constants.ID)
            serializer = ConnectorsMapCreateSerializer(data=maps)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        return Response(connectors_data, status=status.HTTP_201_CREATED)
    @http_request_mutation
    def list(self, request, *args, **kwargs):
        data = Connectors.objects.all().filter(user_id=request.META.get("user_id"), user__status=True).order_by(
            Constants.UPDATED_AT).reverse()
        page = self.paginate_queryset(data)
        connectors_data = ConnectorsListSerializer(page, many=True)
        return self.get_paginated_response(connectors_data.data)

    @http_request_mutation
    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        instance = self.get_object()
        serializer = ConnectorsRetriveSerializer(instance=instance)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @transaction.atomic
    @authenticate_user(model=Connectors)
    def update(self, request, pk):
        """PUT method: update or send a PUT request on an object of the Product model"""
        instance = self.get_object()
        data = request.data
        temp_path = f"{settings.TEMP_CONNECTOR_URL}{data.get(Constants.NAME)}.csv"
        dest_path = f"{settings.CONNECTOR_FILES_URL}{data.get(Constants.NAME)}.csv"
        data.pop(Constants.INTEGRATED_FILE)
        if os.path.exists(temp_path):
            shutil.move(temp_path, dest_path)
        connector_serializer = ConnectorsCreateSerializer(instance, data=request.data, partial=True)
        connector_serializer.is_valid(raise_exception=True)
        connector_serializer.save()
        Connectors.objects.filter(id=connector_serializer.data.get(Constants.ID)
                                  ).update(integrated_file=dest_path)
        for maps in request.data.get(Constants.MAPS, []):
            maps[Constants.CONNECTORS] = pk
            if maps.get(Constants.ID):
                instance = ConnectorsMap.objects.get(id=maps.get(Constants.ID))
                serializer = ConnectorsMapCreateSerializer(instance, data=maps, partial=True)
            else:
                serializer = ConnectorsMapCreateSerializer(data=maps)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        return Response(connector_serializer.data, status=status.HTTP_200_OK)

    def destroy(self, request, pk):
        """DELETE method: delete an object"""
        if request.GET.get(Constants.MAPS):
            connector = ConnectorsMap.objects.get(id=pk)
            connector.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)
        else:
            connector = self.get_object()
            connector.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=["post"])
    def integration(self, request, *args, **kwargs):
        data = request.data
        maps = request.data.get(Constants.MAPS)
        integrated_file = request.GET.get(Constants.INTEGRATED_FILE, '')
        if integrated_file:
            integrated_file = str(integrated_file).replace("media/", "").replace("%20", " ")
            size = os.path.getsize(os.path.join(settings.MEDIA_ROOT, integrated_file))
            if size > Constants.MAX_CONNECTOR_FILE:
                return Response({f"Integrated data exceeds maximum limit: {size / 1000000} MB"}, status=500)
        if not request.GET.get(Constants.EDIT, False):
            serializer = ConnectorsCreateSerializer(data=data)
            serializer.is_valid(raise_exception=True)
        if not maps:
            return Response({f"Minimum 2 datasets should select for integration"}, status=500)
        integrate = maps[0]
        try:
            left_dataset_file_path = unquote(integrate.get(Constants.LEFT_DATASET_FILE_PATH)
                                             ).replace(Constants.SLASH_MEDIA_SLASH, "")
            right_dataset_file_path = unquote(integrate.get(Constants.RIGHT_DATASET_FILE_PATH)
                                              ).replace(Constants.SLASH_MEDIA_SLASH, "")
            condition = integrate.get(Constants.CONDITION)

            if left_dataset_file_path.endswith(".xlsx") or left_dataset_file_path.endswith(".xls"):
                left_dataset = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, left_dataset_file_path),
                                             usecols=condition.get(Constants.LEFT_SELECTED))
            else:
                left_dataset = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, left_dataset_file_path),
                                           usecols=condition.get(Constants.LEFT_SELECTED))
            if right_dataset_file_path.endswith(".xlsx") or right_dataset_file_path.endswith(".xls"):
                right_dataset = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, right_dataset_file_path),
                                              usecols=condition.get(Constants.RIGHT_SELECTED))
            else:
                right_dataset = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, right_dataset_file_path),
                                            usecols=condition.get(Constants.RIGHT_SELECTED))
            # Join the dataframes
            result = pd.merge(
                left_dataset,
                right_dataset,
                how=condition.get(Constants.HOW, Constants.LEFT),
                left_on=condition.get(Constants.LEFT_ON),
                right_on=condition.get(Constants.RIGHT_ON),
            )
            for i in range(1, len(maps)):
                integrate = maps[i]
                right_dataset_file_path = unquote(integrate.get(Constants.RIGHT_DATASET_FILE_PATH)
                                                  ).replace(Constants.SLASH_MEDIA_SLASH, "")
                condition = integrate.get(Constants.CONDITION)
                if right_dataset_file_path.endswith(".xlsx") or right_dataset_file_path.endswith(".xls"):
                    right_dataset = pd.read_excel(
                        os.path.join(settings.DATASET_FILES_URL, right_dataset_file_path),
                        usecols=condition.get(Constants.RIGHT_SELECTED)
                    )
                else:
                    right_dataset = pd.read_csv(
                        os.path.join(settings.DATASET_FILES_URL, right_dataset_file_path),
                        usecols=condition.get(Constants.RIGHT_SELECTED)
                    )
                # Join the dataframes
                result = pd.merge(
                    result,
                    right_dataset,
                    how=condition.get(Constants.HOW, Constants.LEFT),
                    left_on=condition.get(Constants.LEFT_ON),
                    right_on=condition.get(Constants.RIGHT_ON),
                )
            name = data.get(Constants.NAME, Constants.CONNECTORS)
            file_path = f"{settings.TEMP_CONNECTOR_URL}{name}.csv"
            result.to_csv(file_path, index=False)
            result_length = len(result)
            if result_length > 20:
                result = result.iloc[:20]
            return Response({Constants.INTEGRATED_FILE: file_path,
                             Constants.DATA: json.loads(result.to_json(orient='table', index=False)),
                             "no_of_records": result_length},
                            status=status.HTTP_200_OK)
        except Exception as e:
            logging.error(str(e), exc_info=True)
            return Response({"message": "To perform column joins in Dathub, Columns should have matching data types"}, status=400)
