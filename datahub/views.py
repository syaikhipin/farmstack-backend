import ast
import csv
from datetime import timedelta, timezone
import json
import logging
import operator
import os
import re
import shutil
import time
import numpy as np
import sys
from calendar import c
from functools import reduce
from pickle import TRUE
from urllib.parse import unquote
import json
import django
from jsonschema import ValidationError
import pandas as pd
from django.conf import settings
from django.contrib.admin.utils import get_model_from_relation
from django.core.files.base import ContentFile
from django.db import transaction

# from django.http import HttpResponse
from django.db.models import (
    DEFERRED,
    CharField,
    Count,
    F,
    Func,
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
)
from django.db.models.functions import Concat
from rest_framework.exceptions import ValidationError

# from django.db.models.functions import Index, Substr
from django.http import JsonResponse
from django.shortcuts import render
from drf_braces.mixins import MultipleSerializersViewMixin
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from python_http_client import exceptions
from rest_framework import generics, pagination, status
from rest_framework.decorators import action
from rest_framework.parsers import JSONParser, MultiPartParser
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet, ViewSet
from uritemplate import partial

from accounts.models import User, UserRole
from accounts.serializers import (
    UserCreateSerializer,
    UserSerializer,
    UserUpdateSerializer,
)
from connectors.models import Connectors
from connectors.serializers import ConnectorsListSerializer
from core.constants import Constants, NumericalConstants
from core.settings import BASE_DIR
from core.utils import (
    CustomPagination,
    Utils,
    csv_and_xlsx_file_validatation,
    date_formater,
    read_contents_from_csv_or_xlsx_file,
)
from datahub.models import (
    DatahubDocuments,
    Datasets,
    DatasetV2,
    DatasetV2File,
    Organization,
    StandardisationTemplate,
    UserOrganizationMap,
    Resource,
)
from datahub.serializers import (
    DatahubDatasetsSerializer,
    DatahubDatasetsV2Serializer,
    DatahubThemeSerializer,
    DatasetFileV2NewSerializer,
    DatasetSerializer,
    DatasetUpdateSerializer,
    DatasetV2DetailNewSerializer,
    DatasetV2ListNewSerializer,
    DatasetV2NewListSerializer,
    DatasetV2Serializer,
    DatasetV2TempFileSerializer,
    DatasetV2Validation,
    DropDocumentSerializer,
    OrganizationSerializer,
    ParticipantSerializer,
    PolicyDocumentSerializer,
    RecentDatasetListSerializer,
    RecentSupportTicketSerializer,
    StandardisationTemplateUpdateSerializer,
    StandardisationTemplateViewSerializer,
    TeamMemberCreateSerializer,
    TeamMemberDetailsSerializer,
    TeamMemberListSerializer,
    TeamMemberUpdateSerializer,
    UserOrganizationCreateSerializer,
    UserOrganizationMapSerializer,
    ResourceSerializer,
)
from participant.models import SupportTicket
from participant.serializers import (
    ParticipantSupportTicketSerializer,
    TicketSupportSerializer,
)
from utils import custom_exceptions, file_operations, string_functions, validators
from utils.authentication_services import authenticate_user
from utils.file_operations import check_file_name_length
from utils.jwt_services import http_request_mutation

from .models import Policy, ResourceFile, UsagePolicy
from .serializers import (
    PolicySerializer,
    ResourceFileSerializer,
    UsagePolicyDetailSerializer,
    UsagePolicySerializer,
    APIBuilderSerializer,
)
from core.utils import generate_api_key

LOGGER = logging.getLogger(__name__)

con = None


class DefaultPagination(pagination.PageNumberPagination):
    """
    Configure Pagination
    """

    page_size = 5


class TeamMemberViewSet(GenericViewSet):
    """Viewset for Product model"""

    serializer_class = TeamMemberListSerializer
    queryset = User.objects.all()
    pagination_class = CustomPagination

    def create(self, request, *args, **kwargs):
        """POST method: create action to save an object by sending a POST request"""
        serializer = TeamMemberCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def list(self, request, *args, **kwargs):
        """GET method: query all the list of objects from the Product model"""
        # queryset = self.filter_queryset(self.get_queryset())
        queryset = User.objects.filter(Q(status=True) & (Q(role__id=2) | Q(role__id=5)))
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        team_member = self.get_object()
        serializer = TeamMemberDetailsSerializer(team_member)
        # serializer.is_valid(raise_exception=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def update(self, request, *args, **kwargs):
        """PUT method: update or send a PUT request on an object of the Product model"""
        instance = self.get_object()
        # request.data["role"] = UserRole.objects.get(role_name=request.data["role"]).id
        serializer = TeamMemberUpdateSerializer(instance, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def destroy(self, request, pk):
        """DELETE method: delete an object"""
        team_member = self.get_object()
        team_member.status = False
        # team_member.delete()
        team_member.save()
        return Response(status=status.HTTP_204_NO_CONTENT)


class OrganizationViewSet(GenericViewSet):
    """
    Organisation Viewset.
    """

    serializer_class = OrganizationSerializer
    queryset = Organization.objects.all()
    pagination_class = CustomPagination
    parser_class = MultiPartParser

    def perform_create(self, serializer):
        """
        This function performs the create operation of requested serializer.
        Args:
            serializer (_type_): serializer class object.

        Returns:
            _type_: Returns the saved details.
        """
        return serializer.save()

    def create(self, request, *args, **kwargs):
        """POST method: create action to save an organization object using User ID (IMPORTANT: Using USER ID instead of Organization ID)"""
        try:
            user_obj = User.objects.get(id=request.data.get(Constants.USER_ID))
            user_org_queryset = UserOrganizationMap.objects.filter(user_id=request.data.get(Constants.USER_ID)).first()
            if user_org_queryset:
                return Response(
                    {"message": ["User is already associated with an organization"]},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            else:
                with transaction.atomic():
                    # create organization and userorganizationmap object
                    print("Creating org & user_org_map")
                    org_serializer = OrganizationSerializer(data=request.data, partial=True)
                    org_serializer.is_valid(raise_exception=True)
                    org_queryset = self.perform_create(org_serializer)

                    user_org_serializer = UserOrganizationMapSerializer(
                        data={
                            Constants.USER: user_obj.id,
                            Constants.ORGANIZATION: org_queryset.id,
                        }  # type: ignore
                    )
                    user_org_serializer.is_valid(raise_exception=True)
                    self.perform_create(user_org_serializer)
                    data = {
                        "user_map": user_org_serializer.data.get("id"),
                        "org_id": org_queryset.id,
                        "organization": org_serializer.data,
                    }
                    return Response(data, status=status.HTTP_201_CREATED)

        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    def list(self, request, *args, **kwargs):
        """GET method: query the list of Organization objects"""
        try:
            user_org_queryset = (
                UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                .filter(organization__status=True)
                .all()
            )
            page = self.paginate_queryset(user_org_queryset)
            user_organization_serializer = ParticipantSerializer(page, many=True)
            return self.get_paginated_response(user_organization_serializer.data)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def retrieve(self, request, pk):
        """GET method: retrieve an object of Organization using User ID of the User (IMPORTANT: Using USER ID instead of Organization ID)"""
        try:
            user_obj = User.objects.get(id=pk, status=True)
            user_org_queryset = UserOrganizationMap.objects.prefetch_related(
                Constants.USER, Constants.ORGANIZATION
            ).filter(organization__status=True, user=pk)

            if not user_org_queryset:
                data = {Constants.USER: {"id": user_obj.id}, Constants.ORGANIZATION: "null"}
                return Response(data, status=status.HTTP_200_OK)

            org_obj = Organization.objects.get(id=user_org_queryset.first().organization_id)
            user_org_serializer = OrganizationSerializer(org_obj)
            data = {
                Constants.USER: {"id": user_obj.id},
                Constants.ORGANIZATION: user_org_serializer.data,
            }
            return Response(data, status=status.HTTP_200_OK)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def update(self, request, pk):
        """PUT method: update or PUT request for Organization using User ID of the User (IMPORTANT: Using USER ID instead of Organization ID)"""
        user_obj = User.objects.get(id=pk, status=True)
        user_org_queryset = (
            UserOrganizationMap.objects.prefetch_related(Constants.USER, Constants.ORGANIZATION).filter(user=pk).all()
        )

        if not user_org_queryset:
            return Response({}, status=status.HTTP_404_NOT_FOUND)  # 310-360 not covered 4

        organization_serializer = OrganizationSerializer(
            Organization.objects.get(id=user_org_queryset.first().organization_id),
            data=request.data,
            partial=True,
        )

        organization_serializer.is_valid(raise_exception=True)
        self.perform_create(organization_serializer)
        data = {
            Constants.USER: {"id": pk},
            Constants.ORGANIZATION: organization_serializer.data,
            "user_map": user_org_queryset.first().id,
            "org_id": user_org_queryset.first().organization_id,
        }
        return Response(
            data,
            status=status.HTTP_201_CREATED,
        )

    def destroy(self, request, pk):
        """DELETE method: delete an object"""
        try:
            user_obj = User.objects.get(id=pk, status=True)
            user_org_queryset = UserOrganizationMap.objects.select_related(Constants.ORGANIZATION).get(user_id=pk)
            org_queryset = Organization.objects.get(id=user_org_queryset.organization_id)
            org_queryset.status = False
            self.perform_create(org_queryset)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ParticipantViewSet(GenericViewSet):
    """
    This class handles the participant CRUD operations.
    """

    parser_class = JSONParser
    serializer_class = UserCreateSerializer
    queryset = User.objects.all()
    pagination_class = CustomPagination

    def perform_create(self, serializer):
        """
        This function performs the create operation of requested serializer.
        Args:
            serializer (_type_): serializer class object.

        Returns:
            _type_: Returns the saved details.
        """
        return serializer.save()

    @authenticate_user(model=Organization)
    @transaction.atomic
    def create(self, request, *args, **kwargs):
        """POST method: create action to save an object by sending a POST request"""
        org_serializer = OrganizationSerializer(data=request.data, partial=True)
        org_serializer.is_valid(raise_exception=True)
        org_queryset = self.perform_create(org_serializer)
        org_id = org_queryset.id
        user_serializer = UserCreateSerializer(data=request.data)
        user_serializer.is_valid(raise_exception=True)
        user_saved = self.perform_create(user_serializer)
        user_org_serializer = UserOrganizationMapSerializer(
            data={
                Constants.USER: user_saved.id,
                Constants.ORGANIZATION: org_id,
            }  # type: ignore
        )
        user_org_serializer.is_valid(raise_exception=True)
        self.perform_create(user_org_serializer)
        try:
            if user_saved.on_boarded_by:
                # datahub_admin = User.objects.filter(id=user_saved.on_boarded_by).first()
                admin_full_name = string_functions.get_full_name(
                    user_saved.on_boarded_by.first_name,
                    user_saved.on_boarded_by.last_name,
                )
            else:
                datahub_admin = User.objects.filter(role_id=1).first()
                admin_full_name = string_functions.get_full_name(datahub_admin.first_name, datahub_admin.last_name)
            participant_full_name = string_functions.get_full_name(
                request.data.get("first_name"), request.data.get("last_name")
            )
            data = {
                Constants.datahub_name: os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
                "as_user": "Co-Steward" if user_saved.role == 6 else "Participant",
                "participant_admin_name": participant_full_name,
                "participant_organization_name": request.data.get("name"),
                "datahub_admin": admin_full_name,
                Constants.datahub_site: os.environ.get(Constants.DATAHUB_SITE, Constants.datahub_site),
            }

            email_render = render(request, Constants.WHEN_DATAHUB_ADMIN_ADDS_PARTICIPANT, data)
            mail_body = email_render.content.decode("utf-8")
            Utils().send_email(
                to_email=request.data.get("email"),
                content=mail_body,
                subject=Constants.PARTICIPANT_ORG_ADDITION_SUBJECT
                + os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
            )
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response({"message": ["An error occured"]}, status=status.HTTP_200_OK)

        return Response(user_org_serializer.data, status=status.HTTP_201_CREATED)

    def list(self, request, *args, **kwargs):
        """GET method: query all the list of objects from the Product model"""
        on_boarded_by = request.GET.get("on_boarded_by", None)
        co_steward = request.GET.get("co_steward", False)
        approval_status = request.GET.get(Constants.APPROVAL_STATUS, True)
        name = request.GET.get(Constants.NAME, "")
        filter = {Constants.ORGANIZATION_NAME_ICONTAINS: name} if name else {}
        if on_boarded_by:
            roles = (
                UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                .filter(
                    user__status=True,
                    user__on_boarded_by=on_boarded_by,
                    user__role=3,
                    user__approval_status=approval_status,
                    **filter,
                )
                .order_by("-user__updated_at")
                .all()
            )
        elif co_steward:
            roles = (
                UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                .filter(user__status=True, user__role=6, **filter)
                .order_by("-user__updated_at")
                .all()
            )
        else:
            roles = (
                UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                .filter(
                    user__status=True,
                    user__role=3,
                    user__on_boarded_by=None,
                    user__approval_status=approval_status,
                    **filter,
                )
                .order_by("-user__updated_at")
                .all()
            )

        page = self.paginate_queryset(roles)
        participant_serializer = ParticipantSerializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)

    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        roles = (
            UserOrganizationMap.objects.prefetch_related(Constants.USER, Constants.ORGANIZATION)
            .filter(user__status=True, user=pk)
            .first()
        )

        participant_serializer = ParticipantSerializer(roles, many=False)
        if participant_serializer.data:
            return Response(participant_serializer.data, status=status.HTTP_200_OK)
        return Response([], status=status.HTTP_200_OK)

    @authenticate_user(model=Organization)
    @transaction.atomic
    def update(self, request, *args, **kwargs):
        """PUT method: update or send a PUT request on an object of the Product model"""
        try:
            participant = self.get_object()
            user_serializer = self.get_serializer(participant, data=request.data, partial=True)
            user_serializer.is_valid(raise_exception=True)
            organization = Organization.objects.get(id=request.data.get(Constants.ID))
            organization_serializer = OrganizationSerializer(organization, data=request.data, partial=True)
            organization_serializer.is_valid(raise_exception=True)
            user_data = self.perform_create(user_serializer)
            self.perform_create(organization_serializer)

            if user_data.on_boarded_by:
                admin_full_name = string_functions.get_full_name(user_data.first_name, user_data.last_name)
            else:
                datahub_admin = User.objects.filter(role_id=1).first()
                admin_full_name = string_functions.get_full_name(datahub_admin.first_name, datahub_admin.last_name)
            participant_full_name = string_functions.get_full_name(participant.first_name, participant.last_name)

            data = {
                Constants.datahub_name: os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
                "participant_admin_name": participant_full_name,
                "participant_organization_name": organization.name,
                "datahub_admin": admin_full_name,
                Constants.datahub_site: os.environ.get(Constants.DATAHUB_SITE, Constants.datahub_site),
            }

            # update data & trigger_email
            email_render = render(request, Constants.DATAHUB_ADMIN_UPDATES_PARTICIPANT_ORGANIZATION, data)
            mail_body = email_render.content.decode("utf-8")
            Utils().send_email(
                to_email=participant.email,
                content=mail_body,
                subject=Constants.PARTICIPANT_ORG_UPDATION_SUBJECT
                + os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
            )

            data = {
                Constants.USER: user_serializer.data,
                Constants.ORGANIZATION: organization_serializer.data,
            }
            return Response(data, status=status.HTTP_201_CREATED)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response({"message": ["An error occured"]}, status=status.HTTP_200_OK)

    @authenticate_user(model=Organization)
    def destroy(self, request, pk):
        """DELETE method: delete an object"""
        participant = self.get_object()
        user_organization = (
            UserOrganizationMap.objects.select_related(Constants.ORGANIZATION).filter(user_id=pk).first()
        )
        organization = Organization.objects.get(id=user_organization.organization_id)
        if participant.status:
            participant.status = False
            try:
                if participant.on_boarded_by:
                    datahub_admin = participant.on_boarded_by
                else:
                    datahub_admin = User.objects.filter(role_id=1).first()
                admin_full_name = string_functions.get_full_name(datahub_admin.first_name, datahub_admin.last_name)
                participant_full_name = string_functions.get_full_name(participant.first_name, participant.last_name)

                data = {
                    Constants.datahub_name: os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
                    "participant_admin_name": participant_full_name,
                    "participant_organization_name": organization.name,
                    "datahub_admin": admin_full_name,
                    Constants.datahub_site: os.environ.get(Constants.DATAHUB_SITE, Constants.datahub_site),
                }

                # delete data & trigger_email
                self.perform_create(participant)
                email_render = render(
                    request,
                    Constants.DATAHUB_ADMIN_DELETES_PARTICIPANT_ORGANIZATION,
                    data,
                )
                mail_body = email_render.content.decode("utf-8")
                Utils().send_email(
                    to_email=participant.email,
                    content=mail_body,
                    subject=Constants.PARTICIPANT_ORG_DELETION_SUBJECT
                    + os.environ.get(Constants.DATAHUB_NAME, Constants.datahub_name),
                )

                # Set the on_boarded_by_id to null if co_steward is deleted
                User.objects.filter(on_boarded_by=pk).update(on_boarded_by=None)

                return Response(
                    {"message": ["Participant deleted"]},
                    status=status.HTTP_204_NO_CONTENT,
                )
            except Exception as error:
                LOGGER.error(error, exc_info=True)
                return Response({"message": ["Internal server error"]}, status=500)

        elif participant.status is False:
            return Response(
                {"message": ["participant/co-steward already deleted"]},
                status=status.HTTP_204_NO_CONTENT,
            )

        return Response({"message": ["Internal server error"]}, status=500)

    @action(detail=False, methods=["post"], permission_classes=[AllowAny])
    def get_list_co_steward(self, request, *args, **kwargs):
        try:
            users = (
                User.objects.filter(role__id=6, status=True)
                .values("id", "userorganizationmap__organization__name")
                .distinct("userorganizationmap__organization__name")
            )

            data = [
                {
                    "user": user["id"],
                    "organization_name": user["userorganizationmap__organization__name"],
                }
                for user in users
            ]
            return Response(data, status=200)
        except Exception as e:
            LOGGER.error(e, exc_info=True)
            return Response({"message": str(e)}, status=500)


class MailInvitationViewSet(GenericViewSet):
    """
    This class handles the mail invitation API views.
    """

    def create(self, request, *args, **kwargs):
        """
        This will send the mail to the requested user with content.
        Args:
            request (_type_): Api request object.

        Returns:
            _type_: Retuns the sucess response with message and status code.
        """
        try:
            email_list = request.data.get("to_email")
            emails_found, emails_not_found = ([] for i in range(2))
            # for email in email_list:
            #     if User.objects.filter(email=email):
            #         emails_found.append(email)
            #     else:
            #         emails_not_found.append(email)
            user = User.objects.filter(role_id=1).first()
            full_name = user.first_name + " " + str(user.last_name) if user.last_name else user.first_name
            data = {
                "datahub_name": os.environ.get("DATAHUB_NAME", "datahub_name"),
                "participant_admin_name": full_name,
                "datahub_site": os.environ.get("DATAHUB_SITE", "datahub_site"),
            }
            # render email from query_email template
            for email in email_list:
                try:
                    email_render = render(request, "datahub_admin_invites_participants.html", data)
                    mail_body = email_render.content.decode("utf-8")
                    Utils().send_email(
                        to_email=[email],
                        content=mail_body,
                        subject=os.environ.get("DATAHUB_NAME", "datahub_name")
                        + Constants.PARTICIPANT_INVITATION_SUBJECT,
                    )
                except Exception as e:
                    emails_not_found.append()
            failed = f"No able to send emails to this emails: {emails_not_found}"
            LOGGER.warning(failed)
            return Response(
                {
                    "message": f"Email successfully sent to {emails_found}",
                    "failed": failed,
                },
                status=status.HTTP_200_OK,
            )

        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(
                {"Error": f"Failed to send email"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )  # type: ignore


class DropDocumentView(GenericViewSet):
    """View for uploading organization document files"""

    parser_class = MultiPartParser
    serializer_class = DropDocumentSerializer

    def create(self, request, *args, **kwargs):
        """Saves the document files in temp location before saving"""
        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)

        try:
            # get file, file name & type from the form-data
            key = list(request.data.keys())[0]
            file = serializer.validated_data[key]
            file_type = str(file).split(".")[-1]
            file_name = str(key) + "." + file_type
            file_operations.remove_files(file_name, settings.TEMP_FILE_PATH)
            file_operations.file_save(file, file_name, settings.TEMP_FILE_PATH)
            return Response(
                {key: [f"{file_name} uploading in progress ..."]},
                status=status.HTTP_201_CREATED,
            )

        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["delete"])
    def delete(self, request):
        """remove the dropped documents"""
        try:
            key = list(request.data.keys())[0]
            file_operations.remove_files(key, settings.TEMP_FILE_PATH)
            return Response({}, status=status.HTTP_204_NO_CONTENT)

        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)


class DocumentSaveView(GenericViewSet):
    """View for uploading all the datahub documents and content"""

    serializer_class = PolicyDocumentSerializer
    queryset = DatahubDocuments.objects.all()

    @action(detail=False, methods=["get"])
    def get(self, request):
        """GET method: retrieve an object or instance of the Product model"""
        try:
            file_paths = file_operations.file_path(settings.DOCUMENTS_URL)
            datahub_obj = DatahubDocuments.objects.last()
            content = {
                Constants.GOVERNING_LAW: datahub_obj.governing_law if datahub_obj else None,
                Constants.PRIVACY_POLICY: datahub_obj.privacy_policy if datahub_obj else None,
                Constants.TOS: datahub_obj.tos if datahub_obj else None,
                Constants.LIMITATIONS_OF_LIABILITIES: datahub_obj.limitations_of_liabilities if datahub_obj else None,
                Constants.WARRANTY: datahub_obj.warranty if datahub_obj else None,
            }

            documents = {
                Constants.GOVERNING_LAW: file_paths.get("governing_law"),
                Constants.PRIVACY_POLICY: file_paths.get("privacy_policy"),
                Constants.TOS: file_paths.get("tos"),
                Constants.LIMITATIONS_OF_LIABILITIES: file_paths.get("limitations_of_liabilities"),
                Constants.WARRANTY: file_paths.get("warranty"),
            }
            if not datahub_obj and not file_paths:
                data = {"content": content, "documents": documents}
                return Response(data, status=status.HTTP_200_OK)
            elif not datahub_obj:
                data = {"content": content, "documents": documents}
                return Response(data, status=status.HTTP_200_OK)
            elif datahub_obj and not file_paths:
                data = {"content": content, "documents": documents}
                return Response(data, status=status.HTTP_200_OK)
            elif datahub_obj and file_paths:
                data = {"content": content, "documents": documents}
                return Response(data, status=status.HTTP_200_OK)

        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_404_NOT_FOUND)

    def create(self, request, *args, **kwargs):
        try:
            serializer = self.get_serializer(data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)

            with transaction.atomic():
                serializer.save()
                # save the document files
                file_operations.create_directory(settings.DOCUMENTS_ROOT, [])
                file_operations.files_move(settings.TEMP_FILE_PATH, settings.DOCUMENTS_ROOT)
                return Response(
                    {"message": "Documents and content saved!"},
                    status=status.HTTP_201_CREATED,
                )
        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["get"])
    def put(self, request, *args, **kwargs):
        """Saves the document content and files"""
        try:
            # instance = self.get_object()
            datahub_obj = DatahubDocuments.objects.last()
            serializer = self.get_serializer(datahub_obj, data=request.data)
            serializer.is_valid(raise_exception=True)

            with transaction.atomic():
                serializer.save()
                file_operations.create_directory(settings.DOCUMENTS_ROOT, [])
                file_operations.files_move(settings.TEMP_FILE_PATH, settings.DOCUMENTS_ROOT)
                return Response(
                    {"message": "Documents and content updated!"},
                    status=status.HTTP_201_CREATED,
                )
        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)


class DatahubThemeView(GenericViewSet):
    """View for modifying datahub branding"""

    parser_class = MultiPartParser
    serializer_class = DatahubThemeSerializer

    def create(self, request, *args, **kwargs):
        """generates the override css for datahub"""
        # user = User.objects.filter(email=request.data.get("email", ""))
        # user = user.first()
        data = {}

        try:
            banner = request.data.get("banner", "null")
            banner = None if banner == "null" else banner
            button_color = request.data.get("button_color", "null")
            button_color = None if button_color == "null" else button_color
            if not banner and not button_color:
                data = {"banner": "null", "button_color": "null"}
            elif banner and not button_color:
                file_name = file_operations.file_rename(str(banner), "banner")
                shutil.rmtree(settings.THEME_ROOT)
                os.mkdir(settings.THEME_ROOT)
                os.makedirs(settings.CSS_ROOT)
                file_operations.file_save(banner, file_name, settings.THEME_ROOT)
                data = {"banner": file_name, "button_color": "null"}

            elif not banner and button_color:
                css = ".btn { background-color: " + button_color + "; }"
                file_operations.remove_files(file_name, settings.THEME_ROOT)
                file_operations.file_save(
                    ContentFile(css),
                    settings.CSS_FILE_NAME,
                    settings.CSS_ROOT,
                )
                data = {"banner": "null", "button_color": settings.CSS_FILE_NAME}

            elif banner and button_color:
                shutil.rmtree(settings.THEME_ROOT)
                os.mkdir(settings.THEME_ROOT)
                os.makedirs(settings.CSS_ROOT)
                file_name = file_operations.file_rename(str(banner), "banner")
                file_operations.remove_files(file_name, settings.THEME_ROOT)
                file_operations.file_save(banner, file_name, settings.THEME_ROOT)

                css = ".btn { background-color: " + button_color + "; }"
                file_operations.remove_files(file_name, settings.THEME_ROOT)
                file_operations.file_save(
                    ContentFile(css),
                    settings.CSS_FILE_NAME,
                    settings.CSS_ROOT,
                )
                data = {"banner": file_name, "button_color": settings.CSS_FILE_NAME}

            # set datahub admin user status to True
            # user.on_boarded = True
            # user.save()
            return Response(data, status=status.HTTP_201_CREATED)

        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["get"])
    def get(self, request):
        """retrieves Datahub Theme attributes"""
        file_paths = file_operations.file_path(settings.THEME_URL)
        # css_path = file_operations.file_path(settings.CSS_ROOT)
        css_path = settings.CSS_ROOT + settings.CSS_FILE_NAME
        data = {}

        try:
            css_attribute = file_operations.get_css_attributes(css_path, "background-color")

            if not css_path and not file_paths:
                data = {"banner": "null", "css": "null"}
            elif not css_path:
                data = {"banner": file_paths, "css": "null"}
            elif css_path and not file_paths:
                data = {"banner": "null", "css": {"btnBackground": css_attribute}}
            elif css_path and file_paths:
                data = {"banner": file_paths, "css": {"btnBackground": css_attribute}}

            return Response(data, status=status.HTTP_200_OK)

        except Exception as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False)
    def put(self, request, *args, **kwargs):
        data = {}
        try:
            banner = request.data.get("banner", "null")
            banner = None if banner == "null" else banner
            button_color = request.data.get("button_color", "null")
            button_color = None if button_color == "null" else button_color

            if banner is None and button_color is None:
                data = {"banner": "null", "button_color": "null"}

            elif banner and button_color is None:
                shutil.rmtree(settings.THEME_ROOT)
                os.mkdir(settings.THEME_ROOT)
                os.makedirs(settings.CSS_ROOT)
                file_name = file_operations.file_rename(str(banner), "banner")
                # file_operations.remove_files(file_name, settings.THEME_ROOT)
                file_operations.file_save(banner, file_name, settings.THEME_ROOT)
                data = {"banner": file_name, "button_color": "null"}

            elif not banner and button_color:
                css = ".btn { background-color: " + button_color + "; }"
                file_operations.remove_files(settings.CSS_FILE_NAME, settings.CSS_ROOT)
                file_operations.file_save(
                    ContentFile(css),
                    settings.CSS_FILE_NAME,
                    settings.CSS_ROOT,
                )
                data = {"banner": "null", "button_color": settings.CSS_FILE_NAME}

            elif banner and button_color:
                shutil.rmtree(settings.THEME_ROOT)
                os.mkdir(settings.THEME_ROOT)
                os.makedirs(settings.CSS_ROOT)
                file_name = file_operations.file_rename(str(banner), "banner")
                # file_operations.remove_files(file_name, settings.THEME_ROOT)
                file_operations.file_save(banner, file_name, settings.THEME_ROOT)

                css = ".btn { background-color: " + button_color + "; }"
                file_operations.remove_files(settings.CSS_FILE_NAME, settings.CSS_ROOT)
                file_operations.file_save(
                    ContentFile(css),
                    settings.CSS_FILE_NAME,
                    settings.CSS_ROOT,
                )
                data = {"banner": file_name, "button_color": settings.CSS_FILE_NAME}

            return Response(data, status=status.HTTP_201_CREATED)

        except exceptions as error:
            LOGGER.error(error, exc_info=True)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)


class SupportViewSet(GenericViewSet):
    """
    This class handles the participant support tickets CRUD operations.
    """

    parser_class = JSONParser
    serializer_class = TicketSupportSerializer
    queryset = SupportTicket
    pagination_class = CustomPagination

    def perform_create(self, serializer):
        """
        This function performs the create operation of requested serializer.
        Args:
            serializer (_type_): serializer class object.

        Returns:
            _type_: Returns the saved details.
        """
        return serializer.save()

    def create(self, request, *args, **kwargs):
        """POST method: create action to save an object by sending a POST request"""
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=["post"])
    def filters_tickets(self, request, *args, **kwargs):
        """This function get the filter args in body. based on the filter args orm filters the data."""
        try:
            data = (
                SupportTicket.objects.select_related(
                    Constants.USER_MAP,
                    Constants.USER_MAP_USER,
                    Constants.USER_MAP_ORGANIZATION,
                )
                .filter(user_map__user__status=True, **request.data)
                .order_by(Constants.UPDATED_AT)
                .reverse()
                .all()
            )
        except django.core.exceptions.FieldError as error:  # type: ignore
            logging.error(f"Error while filtering the ticketd ERROR: {error}")
            return Response(f"Invalid filter fields: {list(request.data.keys())}", status=400)

        page = self.paginate_queryset(data)
        participant_serializer = ParticipantSupportTicketSerializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)

    def update(self, request, *args, **kwargs):
        """PUT method: update or send a PUT request on an object of the Product model"""
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def list(self, request, *args, **kwargs):
        """GET method: query all the list of objects from the Product model"""
        data = (
            SupportTicket.objects.select_related(
                Constants.USER_MAP,
                Constants.USER_MAP_USER,
                Constants.USER_MAP_ORGANIZATION,
            )
            .filter(user_map__user__status=True, **request.GET)
            .order_by(Constants.UPDATED_AT)
            .reverse()
            .all()
        )
        page = self.paginate_queryset(data)
        participant_serializer = ParticipantSupportTicketSerializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)

    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        data = (
            SupportTicket.objects.select_related(
                Constants.USER_MAP,
                Constants.USER_MAP_USER,
                Constants.USER_MAP_ORGANIZATION,
            )
            .filter(user_map__user__status=True, id=pk)
            .all()
        )
        participant_serializer = ParticipantSupportTicketSerializer(data, many=True)
        if participant_serializer.data:
            return Response(participant_serializer.data[0], status=status.HTTP_200_OK)
        return Response([], status=status.HTTP_200_OK)


class DatahubDatasetsViewSet(GenericViewSet):
    """
    This class handles the participant Datsets CRUD operations.
    """

    parser_class = JSONParser
    serializer_class = DatasetSerializer
    queryset = Datasets
    pagination_class = CustomPagination

    def perform_create(self, serializer):
        """
        This function performs the create operation of requested serializer.
        Args:
            serializer (_type_): serializer class object.

        Returns:
            _type_: Returns the saved details.
        """
        return serializer.save()

    def trigger_email(self, request, template, to_email, subject, first_name, last_name, dataset_name):
        # trigger email to the participant as they are being added
        try:
            datahub_admin = User.objects.filter(role_id=1).first()
            admin_full_name = string_functions.get_full_name(datahub_admin.first_name, datahub_admin.last_name)
            participant_full_name = string_functions.get_full_name(first_name, last_name)

            data = {
                "datahub_name": os.environ.get("DATAHUB_NAME", "datahub_name"),
                "participant_admin_name": participant_full_name,
                "datahub_admin": admin_full_name,
                "dataset_name": dataset_name,
                "datahub_site": os.environ.get("DATAHUB_SITE", "datahub_site"),
            }

            email_render = render(request, template, data)
            mail_body = email_render.content.decode("utf-8")
            Utils().send_email(
                to_email=to_email,
                content=mail_body,
                subject=subject + os.environ.get("DATAHUB_NAME", "datahub_name"),
            )

        except Exception as error:
            LOGGER.error(error, exc_info=True)

    def create(self, request, *args, **kwargs):
        """POST method: create action to save an object by sending a POST request"""
        setattr(request.data, "_mutable", True)
        data = request.data

        if not data.get("is_public"):
            if not csv_and_xlsx_file_validatation(request.data.get(Constants.SAMPLE_DATASET)):
                return Response(
                    {
                        Constants.SAMPLE_DATASET: [
                            "Invalid Sample dataset file (or) Atleast 5 rows should be available. please upload valid file"
                        ]
                    },
                    400,
                )
        data[Constants.APPROVAL_STATUS] = Constants.APPROVED
        serializer = self.get_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @http_request_mutation
    def list(self, request, *args, **kwargs):
        """GET method: query all the list of objects from the Product model"""
        try:
            data = []
            user_id = request.META.get(Constants.USER_ID)
            others = request.data.get(Constants.OTHERS)
            filters = {Constants.USER_MAP_USER: user_id} if user_id and not others else {}
            exclude = {Constants.USER_MAP_USER: user_id} if others else {}
            if exclude or filters:
                data = (
                    Datasets.objects.select_related(
                        Constants.USER_MAP,
                        Constants.USER_MAP_USER,
                        Constants.USER_MAP_ORGANIZATION,
                    )
                    .filter(user_map__user__status=True, status=True, **filters)
                    .exclude(**exclude)
                    .order_by(Constants.UPDATED_AT)
                    .reverse()
                    .all()
                )
            page = self.paginate_queryset(data)
            participant_serializer = DatahubDatasetsSerializer(page, many=True)
            return self.get_paginated_response(participant_serializer.data)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    def retrieve(self, request, pk):
        """GET method: retrieve an object or instance of the Product model"""
        data = (
            Datasets.objects.select_related(
                Constants.USER_MAP,
                Constants.USER_MAP_USER,
                Constants.USER_MAP_ORGANIZATION,
            )
            .filter(user_map__user__status=True, status=True, id=pk)
            .all()
        )
        participant_serializer = DatahubDatasetsSerializer(data, many=True)
        if participant_serializer.data:
            data = participant_serializer.data[0]
            if not data.get("is_public"):
                data[Constants.CONTENT] = read_contents_from_csv_or_xlsx_file(data.get(Constants.SAMPLE_DATASET))
            return Response(data, status=status.HTTP_200_OK)
        return Response({}, status=status.HTTP_200_OK)

    def update(self, request, *args, **kwargs):
        """PUT method: update or send a PUT request on an object of the Product model"""
        setattr(request.data, "_mutable", True)
        data = request.data
        data = {key: value for key, value in data.items() if value != "null"}
        if not data.get("is_public"):
            if data.get(Constants.SAMPLE_DATASET):
                if not csv_and_xlsx_file_validatation(data.get(Constants.SAMPLE_DATASET)):
                    return Response(
                        {
                            Constants.SAMPLE_DATASET: [
                                "Invalid Sample dataset file (or) Atleast 5 rows should be available. please upload valid file"
                            ]
                        },
                        400,
                    )
        category = data.get(Constants.CATEGORY)
        if category:
            data[Constants.CATEGORY] = json.loads(category) if isinstance(category, str) else category
        instance = self.get_object()

        # trigger email to the participant
        user_map_queryset = UserOrganizationMap.objects.select_related(Constants.USER).get(id=instance.user_map_id)
        user_obj = user_map_queryset.user

        # reset the approval status b/c the user modified the dataset after an approval
        if getattr(instance, Constants.APPROVAL_STATUS) == Constants.APPROVED and (
            user_obj.role_id == 3 or user_obj.role_id == 4
        ):
            data[Constants.APPROVAL_STATUS] = Constants.AWAITING_REVIEW

        serializer = DatasetUpdateSerializer(instance, data=data, partial=True)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)

        data = request.data

        if data.get(Constants.APPROVAL_STATUS) == Constants.APPROVED:
            self.trigger_email(
                request,
                "datahub_admin_approves_dataset.html",
                user_obj.email,
                Constants.APPROVED_NEW_DATASET_SUBJECT,
                user_obj.first_name,
                user_obj.last_name,
                instance.name,
            )

        elif data.get(Constants.APPROVAL_STATUS) == Constants.REJECTED:
            self.trigger_email(
                request,
                "datahub_admin_rejects_dataset.html",
                user_obj.email,
                Constants.REJECTED_NEW_DATASET_SUBJECT,
                user_obj.first_name,
                user_obj.last_name,
                instance.name,
            )

        elif data.get(Constants.IS_ENABLED) == str(True) or data.get(Constants.IS_ENABLED) == str("true"):
            self.trigger_email(
                request,
                "datahub_admin_enables_dataset.html",
                user_obj.email,
                Constants.ENABLE_DATASET_SUBJECT,
                user_obj.first_name,
                user_obj.last_name,
                instance.name,
            )

        elif data.get(Constants.IS_ENABLED) == str(False) or data.get(Constants.IS_ENABLED) == str("false"):
            self.trigger_email(
                request,
                "datahub_admin_disables_dataset.html",
                user_obj.email,
                Constants.DISABLE_DATASET_SUBJECT,
                user_obj.first_name,
                user_obj.last_name,
                instance.name,
            )

        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def destroy(self, request, pk):
        """DELETE method: delete an object"""
        dataset = self.get_object()
        dataset.status = False
        self.perform_create(dataset)
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=["post"])
    def dataset_filters(self, request, *args, **kwargs):
        """This function get the filter args in body. based on the filter args orm filters the data."""
        data = request.data
        org_id = data.pop(Constants.ORG_ID, "")
        others = data.pop(Constants.OTHERS, "")
        user_id = data.pop(Constants.USER_ID, "")
        categories = data.pop(Constants.CATEGORY, None)
        exclude, filters = {}, {}
        if others:
            exclude = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
        else:
            filters = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}

        try:
            if categories is not None:
                data = (
                    Datasets.objects.select_related(
                        Constants.USER_MAP,
                        Constants.USER_MAP_USER,
                        Constants.USER_MAP_ORGANIZATION,
                    )
                    .filter(status=True, **data, **filters)
                    .filter(
                        reduce(
                            operator.or_,
                            (Q(category__contains=cat) for cat in categories),
                        )
                    )
                    .exclude(**exclude)
                    .order_by(Constants.UPDATED_AT)
                    .reverse()
                    .all()
                )

            else:
                data = (
                    Datasets.objects.select_related(
                        Constants.USER_MAP,
                        Constants.USER_MAP_USER,
                        Constants.USER_MAP_ORGANIZATION,
                    )
                    .filter(status=True, **data, **filters)
                    .exclude(**exclude)
                    .order_by(Constants.UPDATED_AT)
                    .reverse()
                    .all()
                )
        except Exception as error:  # type: ignore
            logging.error("Error while filtering the datasets. ERROR: %s", error)
            return Response(f"Invalid filter fields: {list(request.data.keys())}", status=500)

        page = self.paginate_queryset(data)
        participant_serializer = DatahubDatasetsSerializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)

    @action(detail=False, methods=["post"])
    @http_request_mutation
    def filters_data(self, request, *args, **kwargs):
        """This function provides the filters data"""
        try:
            data = request.data
            org_id = data.pop(Constants.ORG_ID, "")
            others = data.pop(Constants.OTHERS, "")
            user_id = data.pop(Constants.USER_ID, "")

            ####

            org_id = request.META.pop(Constants.ORG_ID, "")
            others = request.META.pop(Constants.OTHERS, "")
            user_id = request.META.pop(Constants.USER_ID, "")

            exclude, filters = {}, {}
            if others:
                exclude = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
                filters = {Constants.APPROVAL_STATUS: Constants.APPROVED}
            else:
                filters = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
            try:
                geography = (
                    Datasets.objects.values_list(Constants.GEOGRAPHY, flat=True)
                    .filter(status=True, **filters)
                    .exclude(geography="null")
                    .exclude(geography__isnull=True)
                    .exclude(geography="")
                    .exclude(**exclude)
                    .all()
                    .distinct()
                )
                crop_detail = (
                    Datasets.objects.values_list(Constants.CROP_DETAIL, flat=True)
                    .filter(status=True, **filters)
                    .exclude(crop_detail="null")
                    .exclude(crop_detail__isnull=True)
                    .exclude(crop_detail="")
                    .exclude(**exclude)
                    .all()
                    .distinct()
                )
                if os.path.exists(Constants.CATEGORIES_FILE):
                    with open(Constants.CATEGORIES_FILE, "r") as json_obj:
                        category_detail = json.loads(json_obj.read())
                else:
                    category_detail = []
            except Exception as error:  # type: ignore
                logging.error("Error while filtering the datasets. ERROR: %s", error)
                return Response(f"Invalid filter fields: {list(request.data.keys())}", status=500)
            return Response(
                {
                    "geography": geography,
                    "crop_detail": crop_detail,
                    "category_detail": category_detail,
                },
                status=200,
            )
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    @http_request_mutation
    def search_datasets(self, request, *args, **kwargs):
        data = request.data
        org_id = data.pop(Constants.ORG_ID, "")
        others = data.pop(Constants.OTHERS, "")
        user_id = data.pop(Constants.USER_ID, "")

        org_id = request.META.pop(Constants.ORG_ID, "")
        others = request.META.pop(Constants.OTHERS, "")
        user_id = request.META.pop(Constants.USER_ID, "")

        search_pattern = data.pop(Constants.SEARCH_PATTERNS, "")
        exclude, filters = {}, {}

        if others:
            exclude = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
            filters = {Constants.NAME_ICONTAINS: search_pattern} if search_pattern else {}
        else:
            filters = (
                {
                    Constants.USER_MAP_ORGANIZATION: org_id,
                    Constants.NAME_ICONTAINS: search_pattern,
                }
                if org_id
                else {}
            )
        try:
            data = (
                Datasets.objects.select_related(
                    Constants.USER_MAP,
                    Constants.USER_MAP_USER,
                    Constants.USER_MAP_ORGANIZATION,
                )
                .filter(user_map__user__status=True, status=True, **data, **filters)
                .exclude(**exclude)
                .order_by(Constants.UPDATED_AT)
                .reverse()
                .all()
            )
        except Exception as error:  # type: ignore
            logging.error("Error while filtering the datasets. ERROR: %s", error)
            return Response(
                f"Invalid filter fields: {list(request.data.keys())}",
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        page = self.paginate_queryset(data)
        participant_serializer = DatahubDatasetsSerializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)


class DatahubDashboard(GenericViewSet):
    """Datahub Dashboard viewset"""

    pagination_class = CustomPagination

    @action(detail=False, methods=["get"])
    def dashboard(self, request):
        """Retrieve datahub dashboard details"""
        try:
            # total_participants = User.objects.filter(role_id=3, status=True).count()
            total_participants = (
                UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                .filter(user__role=3, user__status=True, is_temp=False)
                .count()
            )
            total_datasets = (
                DatasetV2.objects.select_related("user_map", "user_map__user", "user_map__organization")
                .filter(user_map__user__status=True, is_temp=False)
                .count()
            )
            # write a function to compute data exchange
            active_connectors = Connectors.objects.filter(status=True).count()
            total_data_exchange = {"total_data": 50, "unit": "Gbs"}

            datasets = Datasets.objects.filter(status=True).values_list("category", flat=True)
            categories = set()
            categories_dict = {}

            for data in datasets:
                if data and type(data) == dict:
                    for element in data.keys():
                        categories.add(element)

            categories_dict = {key: 0 for key in categories}
            for data in datasets:
                if data and type(data) == dict:
                    for key, value in data.items():
                        if value == True:
                            categories_dict[key] += 1

            open_support_tickets = SupportTicket.objects.filter(status="open").count()
            closed_support_tickets = SupportTicket.objects.filter(status="closed").count()
            hold_support_tickets = SupportTicket.objects.filter(status="hold").count()

            # retrieve 3 recent support tickets
            recent_tickets_queryset = SupportTicket.objects.order_by("updated_at")[0:3]
            recent_tickets_serializer = RecentSupportTicketSerializer(recent_tickets_queryset, many=True)
            support_tickets = {
                "open_requests": open_support_tickets,
                "closed_requests": closed_support_tickets,
                "hold_requests": hold_support_tickets,
                "recent_tickets": recent_tickets_serializer.data,
            }

            # retrieve 3 recent updated datasets
            # datasets_queryset = Datasets.objects.order_by("updated_at")[0:3]
            datasets_queryset = Datasets.objects.filter(status=True).order_by("-updated_at").all()
            datasets_queryset_pages = self.paginate_queryset(datasets_queryset)  # paginaged connectors list
            datasets_serializer = RecentDatasetListSerializer(datasets_queryset_pages, many=True)

            data = {
                "total_participants": total_participants,
                "total_datasets": total_datasets,
                "active_connectors": active_connectors,
                "total_data_exchange": total_data_exchange,
                "categories": categories_dict,
                "support_tickets": support_tickets,
                "datasets": self.get_paginated_response(datasets_serializer.data).data,
            }
            return Response(data, status=status.HTTP_200_OK)

        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response({}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DatasetV2ViewSet(GenericViewSet):
    """
    ViewSet for DatasetV2 model for create, update, detail/list view, & delete endpoints.

    **Context**
    ``DatasetV2``
        An instance of :model:`datahub_datasetv2`

    **Serializer**
    ``DatasetV2Serializer``
        :serializer:`datahub.serializer.DatasetV2Serializer`

    **Authorization**
        ``ROLE`` only authenticated users/participants with following roles are allowed to make a POST request to this endpoint.
            :role: `datahub_admin` (:role_id: `1`)
            :role: `datahub_participant_root` (:role_id: `3`)
    """

    serializer_class = DatasetV2Serializer
    queryset = DatasetV2.objects.all()
    pagination_class = CustomPagination

    @action(detail=False, methods=["post"])
    def validate_dataset(self, request, *args, **kwargs):
        """
        ``POST`` method Endpoint: POST method to check the validation of dataset name and dataset description. [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/dataset_validation/
        """
        serializer = DatasetV2Validation(
            data=request.data,
            context={
                "request_method": request.method,
                "dataset_exists": request.query_params.get("dataset_exists"),
                "queryset": self.queryset,
            },
        )
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post", "delete"])
    def temp_datasets(self, request, *args, **kwargs):
        """
        ``POST`` method Endpoint: POST method to save the datasets in a temporary location with
            under a newly created dataset name & source_file directory.
        ``DELETE`` method Endpoint: DELETE method to delete the dataset named directory containing
            the datasets. [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/temp_datasets/
        """
        try:
            files = request.FILES.getlist("datasets")

            if request.method == "POST":
                """Create a temporary directory containing dataset files uploaded as source.
                ``Example:``
                    Create below directories with dataset files uploaded
                    /temp/<dataset-name>/file/<files>
                """
                # serializer = DatasetV2TempFileSerializer(data=request.data, context={"request_method": request.method})
                serializer = DatasetV2TempFileSerializer(
                    data=request.data,
                    context={
                        "request_method": request.method,
                        "dataset_exists": request.query_params.get("dataset_exists"),
                        "queryset": self.queryset,
                    },
                )
                if not serializer.is_valid():
                    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

                directory_created = file_operations.create_directory(
                    settings.TEMP_DATASET_URL,
                    [
                        serializer.data.get("dataset_name"),
                        serializer.data.get("source"),
                    ],
                )

                files_saved = []
                for file in files:
                    file_operations.file_save(file, file.name, directory_created)
                    files_saved.append(file.name)

                data = {"datasets": files_saved}
                data.update(serializer.data)
                return Response(data, status=status.HTTP_201_CREATED)

            elif request.method == "DELETE":
                """
                Delete the temporary directory containing datasets created by the POST endpoint
                with the dataset files uploaded as source.
                ``Example:``
                    Delete the below directory:
                    /temp/<dataset-name>/
                """
                serializer = DatasetV2TempFileSerializer(
                    data=request.data,
                    context={
                        "request_method": request.method,
                        "query_params": request.query_params.get("delete_dir"),
                    },
                )

                if not serializer.is_valid():
                    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

                directory = string_functions.format_dir_name(
                    settings.TEMP_DATASET_URL, [request.data.get("dataset_name")]
                )

                """Delete directory temp directory as requested"""
                if request.query_params.get("delete_dir") and os.path.exists(directory):
                    shutil.rmtree(directory)
                    LOGGER.info(f"Deleting directory: {directory}")
                    data = {request.data.get("dataset_name"): "Dataset not created"}
                    return Response(data, status=status.HTTP_204_NO_CONTENT)

                elif not request.query_params.get("delete_dir"):
                    """Delete a single file as requested"""
                    file_name = request.data.get("file_name")
                    file_path = os.path.join(directory, request.data.get("source"), file_name)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        LOGGER.info(f"Deleting file: {file_name}")
                        data = {file_name: "File deleted"}
                        return Response(data, status=status.HTTP_204_NO_CONTENT)

                return Response(status=status.HTTP_204_NO_CONTENT)

        except Exception as error:
            LOGGER.error(error, exc_info=True)
        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["get"])
    def get_dataset_files(self, request, *args, **kwargs):
        """
        Get list of dataset files from temporary location.
        """
        try:
            # files_list = file_operations.get_csv_or_xls_files_from_directory(settings.TEMP_DATASET_URL + request.query_params.get(Constants.DATASET_NAME))
            dataset = request.data.get("dataset")
            queryset = DatasetV2File.objects.filter(dataset=dataset)
            serializer = DatasetFileV2NewSerializer(queryset, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as error:
            return Response(f"No such dataset created {error}", status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    def get_dataset_file_columns(self, request, *args, **kwargs):
        """
        To retrieve the list of columns of a dataset file from temporary location
        """
        try:
            dataset_file = DatasetV2File.objects.get(id=request.data.get("id"))
            file_path = str(dataset_file.file)
            if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                df = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=None)
            else:
                df = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=False)
            df.columns = df.columns.astype(str)
            result = df.columns.tolist()
            return Response(result, status=status.HTTP_200_OK)
        except Exception as error:
            LOGGER.error(f"Cannot get the columns of the selected file: {error}")
            return Response(
                f"Cannot get the columns of the selected file: {error}",
                status=status.HTTP_400_BAD_REQUEST,
            )

    @action(detail=False, methods=["post"])
    def standardise(self, request, *args, **kwargs):
        """
        Method to standardise a dataset and generate a file along with it.
        """

        # 1. Take the standardisation configuration variables.
        try:
            standardisation_configuration = request.data.get("standardisation_configuration")
            mask_columns = request.data.get("mask_columns")
            file_path = request.data.get("file_path")
            is_standardised = request.data.get("is_standardised", None)

            if is_standardised:
                file_path = file_path.replace("/standardised", "/datasets")

            if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                df = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=None)
            else:
                df = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=False)

            df["status"] = True
            df.loc[df["status"] == True, mask_columns] = "######"
            # df[mask_columns] = df[mask_columns].mask(True)
            del df["status"]
            # print()
            df.rename(columns=standardisation_configuration, inplace=True)
            df.columns = df.columns.astype(str)
            file_dir = file_path.split("/")
            standardised_dir_path = "/".join(file_dir[-3:-1])
            file_name = file_dir[-1]
            if not os.path.exists(os.path.join(settings.TEMP_STANDARDISED_DIR, standardised_dir_path)):
                os.makedirs(os.path.join(settings.TEMP_STANDARDISED_DIR, standardised_dir_path))
            # print(df)
            if file_name.endswith(".csv"):
                df.to_csv(
                    os.path.join(settings.TEMP_STANDARDISED_DIR, standardised_dir_path, file_name)
                )  # type: ignore
            else:
                df.to_excel(
                    os.path.join(settings.TEMP_STANDARDISED_DIR, standardised_dir_path, file_name)
                )  # type: ignore
            return Response(
                {"standardised_file_path": f"{standardised_dir_path}/{file_name}"},
                status=status.HTTP_200_OK,
            )

        except Exception as error:
            LOGGER.error(f"Could not standardise {error}")
            return Response(error, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["get", "post"])
    def category(self, request, *args, **kwargs):
        """
        ``GET`` method: GET method to retrieve the dataset category & sub categories from JSON file obj
        ``POST`` method: POST method to create and/or edit the dataset categories &
            sub categories and finally write it to JSON file obj. [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/category/
        [JSON File Object]: "/categories.json"
        """
        if request.method == "GET":
            try:
                with open(Constants.CATEGORIES_FILE, "r") as json_obj:
                    data = json.loads(json_obj.read())
                return Response(data, status=status.HTTP_200_OK)
            except Exception as error:
                LOGGER.error(error, exc_info=True)
                raise custom_exceptions.NotFoundException(detail="Categories not found")
        elif request.method == "POST":
            try:
                data = request.data
                with open(Constants.CATEGORIES_FILE, "w+", encoding="utf8") as json_obj:
                    json.dump(data, json_obj, ensure_ascii=False)
                    LOGGER.info(f"Updated Categories: {Constants.CATEGORIES_FILE}")
                return Response(data, status=status.HTTP_201_CREATED)
            except Exception as error:
                LOGGER.error(error, exc_info=True)
                raise exceptions.InternalServerError("Internal Server Error")

    def create(self, request, *args, **kwargs):
        """
        ``POST`` method Endpoint: create action to save the Dataset's Meta data
            with datasets sent through POST request. [see here][ref].

        **Endpoint**
        [ref]: /datahub/dataset/v2/
        """
        serializer = self.get_serializer(
            data=request.data,
            context={
                "standardisation_template": request.data.get("standardisation_template"),
                "standardisation_config": request.data.get("standardisation_config"),
            },
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @authenticate_user(model=DatasetV2)
    def update(self, request, pk, *args, **kwargs):
        """
        ``PUT`` method: PUT method to edit or update the dataset (DatasetV2) and its files (DatasetV2File). [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/<uuid>
        """
        # setattr(request.data, "_mutable", True)
        try:
            data = request.data
            to_delete = ast.literal_eval(data.get("deleted", "[]"))
            self.dataset_files(data, to_delete)
            datasetv2 = self.get_object()
            serializer = self.get_serializer(
                datasetv2,
                data=data,
                partial=True,
                context={
                    "standardisation_template": request.data.get("standardisation_template"),
                    "standardisation_config": request.data.get("standardisation_config"),
                },
            )
            serializer.is_valid(raise_exception=True)
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    # not being used
    @action(detail=False, methods=["delete"])
    def dataset_files(self, request, id=[]):
        """
        ``DELETE`` method: DELETE method to delete the dataset files (DatasetV2File) referenced by DatasetV2 model. [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/dataset_files/
        """
        ids = {}
        for file_id in id:
            dataset_file = DatasetV2File.objects.filter(id=int(file_id))
            if dataset_file.exists():
                LOGGER.info(f"Deleting file: {dataset_file[0].id}")
                file_path = os.path.join("media", str(dataset_file[0].file))
                if os.path.exists(file_path):
                    os.remove(file_path)
                dataset_file.delete()
                ids[file_id] = "File deleted"
        return Response(ids, status=status.HTTP_204_NO_CONTENT)

    def list(self, request, *args, **kwargs):
        """
        ``GET`` method Endpoint: list action to view the list of Datasets via GET request. [see here][ref].

        **Endpoint**
        [ref]: /datahub/dataset/v2/
        """
        queryset = self.get_queryset()
        # serializer = self.get_serializer(queryset, many=True)
        # return Response(serializer.data, status=status.HTTP_200_OK)
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        return Response([], status=status.HTTP_404_NOT_FOUND)

    def retrieve(self, request, pk=None, *args, **kwargs):
        """
        ``GET`` method Endpoint: retrieve action for the detail view of Dataset via GET request
            Returns dataset object view with content of XLX/XLSX file and file URLS. [see here][ref].

        **Endpoint**
        [ref]: /datahub/dataset/v2/<id>/
        """
        user_map = request.GET.get("user_map")
        obj = self.get_object()
        serializer = self.get_serializer(obj).data
        dataset_file_obj = DatasetV2File.objects.prefetch_related("dataset_v2_file").filter(dataset_id=obj.id)
        data = []
        for file in dataset_file_obj:
            path_ = os.path.join(settings.DATASET_FILES_URL, str(file.standardised_file))
            file_path = {}
            file_path["id"] = file.id
            file_path["content"] = read_contents_from_csv_or_xlsx_file(
                os.path.join(settings.DATASET_FILES_URL, str(file.standardised_file))
            )
            file_path["file"] = path_
            file_path["source"] = file.source
            file_path["file_size"] = file.file_size
            file_path["accessibility"] = file.accessibility
            file_path["standardised_file"] = os.path.join(settings.DATASET_FILES_URL, str(file.standardised_file))
            file_path["standardisation_config"] = file.standardised_configuration
            if not user_map:
                usage_policy = UsagePolicyDetailSerializer(file.dataset_v2_file.all(), many=True).data
            else:
                usage_policy = (
                    file.dataset_v2_file.filter(user_organization_map=user_map).order_by("-updated_at").first()
                )

                usage_policy = UsagePolicyDetailSerializer(usage_policy).data if usage_policy else {}
            file_path["usage_policy"] = usage_policy
            data.append(file_path)

        serializer["datasets"] = data
        return Response(serializer, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"])
    def dataset_filters(self, request, *args, **kwargs):
        """This function get the filter args in body. based on the filter args orm filters the data."""
        data = request.data
        org_id = data.pop(Constants.ORG_ID, "")
        others = data.pop(Constants.OTHERS, "")
        categories = data.pop(Constants.CATEGORY, None)
        user_id = data.pop(Constants.USER_ID, "")
        on_boarded_by = data.pop("on_boarded_by", "")
        exclude_filters, filters = {}, {}
        if others:
            exclude_filters = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
        else:
            filters = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
        try:
            data = (
                DatasetV2.objects.select_related(
                    Constants.USER_MAP,
                    Constants.USER_MAP_USER,
                    Constants.USER_MAP_ORGANIZATION,
                )
                .filter(**data, **filters)
                .exclude(is_temp=True)
                .exclude(**exclude_filters)
                .order_by(Constants.UPDATED_AT)
                .reverse()
                .all()
            )
            if categories is not None:
                data = data.filter(
                    reduce(
                        operator.or_,
                        (Q(category__contains=cat) for cat in categories),
                    )
                )
            if on_boarded_by:
                data = (
                    data.filter(user_map__user__on_boarded_by=user_id)
                    if others
                    else data.filter(user_map__user_id=user_id)
                )
            else:
                user_onboarded_by = User.objects.get(id=user_id).on_boarded_by
                if user_onboarded_by:
                    data = (
                        data.filter(
                            Q(user_map__user__on_boarded_by=user_onboarded_by.id)
                            | Q(user_map__user_id=user_onboarded_by.id)
                        )
                        if others
                        else data.filter(user_map__user_id=user_id)
                    )
                else:
                    data = (
                        data.filter(user_map__user__on_boarded_by=None).exclude(user_map__user__role_id=6)
                        if others
                        else data
                    )
        except Exception as error:  # type: ignore
            logging.error("Error while filtering the datasets. ERROR: %s", error, exc_info=True)
            return Response(f"Invalid filter fields: {list(request.data.keys())}", status=500)
        page = self.paginate_queryset(data)
        participant_serializer = DatahubDatasetsV2Serializer(page, many=True)
        return self.get_paginated_response(participant_serializer.data)

    @action(detail=False, methods=["post"])
    @http_request_mutation
    def filters_data(self, request, *args, **kwargs):
        """This function provides the filters data"""
        data = request.META
        org_id = data.pop(Constants.ORG_ID, "")
        others = data.pop(Constants.OTHERS, "")
        user_id = data.pop(Constants.USER_ID, "")

        org_id = request.META.pop(Constants.ORG_ID, "")
        others = request.META.pop(Constants.OTHERS, "")
        user_id = request.META.pop(Constants.USER_ID, "")

        exclude, filters = {}, {}
        if others:
            exclude = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
            # filters = {Constants.APPROVAL_STATUS: Constants.APPROVED}
        else:
            filters = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
        try:
            geography = (
                DatasetV2.objects.values_list(Constants.GEOGRAPHY, flat=True)
                .filter(**filters)
                .exclude(geography="null")
                .exclude(geography__isnull=True)
                .exclude(geography="")
                .exclude(is_temp=True, **exclude)
                .all()
                .distinct()
            )
            # crop_detail = (
            #     Datasets.objects.values_list(Constants.CROP_DETAIL, flat=True)
            #     .filter(status=True, **filters)
            #     .exclude(crop_detail="null")
            #     .exclude(crop_detail__isnull=True)
            #     .exclude(crop_detail="")
            #     .exclude(**exclude)
            #     .all()
            #     .distinct()
            # )
            if os.path.exists(Constants.CATEGORIES_FILE):
                with open(Constants.CATEGORIES_FILE, "r") as json_obj:
                    category_detail = json.loads(json_obj.read())
            else:
                category_detail = []
        except Exception as error:  # type: ignore
            logging.error("Error while filtering the datasets. ERROR: %s", error)
            return Response(f"Invalid filter fields: {list(request.data.keys())}", status=500)
        return Response({"geography": geography, "category_detail": category_detail}, status=200)

    # @action(detail=False, methods=["post"])
    # def search_datasets(self, request, *args, **kwargs):
    #     data = request.data
    #     org_id = data.pop(Constants.ORG_ID, "")
    #     others = data.pop(Constants.OTHERS, "")
    #     user_id = data.pop(Constants.USER_ID, "")
    #     search_pattern = data.pop(Constants.SEARCH_PATTERNS, "")
    #     exclude, filters = {}, {}

    #     if others:
    #         exclude = {Constants.USER_MAP_ORGANIZATION: org_id} if org_id else {}
    #         filters = {Constants.NAME_ICONTAINS: search_pattern} if search_pattern else {}
    #     else:
    #         filters = (
    #             {
    #                 Constants.USER_MAP_ORGANIZATION: org_id,
    #                 Constants.NAME_ICONTAINS: search_pattern,
    #             }
    #             if org_id
    #             else {}
    #         )
    #     try:
    #         data = (
    #             DatasetV2.objects.select_related(
    #                 Constants.USER_MAP,
    #                 Constants.USER_MAP_USER,
    #                 Constants.USER_MAP_ORGANIZATION,
    #             )
    #             .filter(user_map__user__status=True, status=True, **data, **filters)
    #             .exclude(**exclude)
    #             .order_by(Constants.UPDATED_AT)
    #             .reverse()
    #             .all()
    #         )
    #         page = self.paginate_queryset(data)
    #         participant_serializer = DatahubDatasetsV2Serializer(page, many=True)
    #         return self.get_paginated_response(participant_serializer.data)
    #     except Exception as error:  # type: ignore
    #         logging.error("Error while filtering the datasets. ERROR: %s", error)
    #         return Response(
    #             f"Invalid filter fields: {list(request.data.keys())}",
    #             status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #         )

    @authenticate_user(model=DatasetV2File)
    def destroy(self, request, pk, *args, **kwargs):
        """
        ``DELETE`` method: DELETE method to delete the DatasetV2 instance and its reference DatasetV2File instances,
        along with dataset files stored at the URL. [see here][ref]

        **Endpoint**
        [ref]: /datahub/dataset/v2/
        """
        try:
            dataset_obj = self.get_object()
            if dataset_obj:
                dataset_files = DatasetV2File.objects.filter(dataset_id=dataset_obj.id)
                dataset_dir = os.path.join(settings.DATASET_FILES_URL, str(dataset_obj.name))

                if os.path.exists(dataset_dir):
                    shutil.rmtree(dataset_dir)
                    LOGGER.info(f"Deleting file: {dataset_dir}")

                # delete DatasetV2File & DatasetV2 instances
                LOGGER.info(f"Deleting dataset obj: {dataset_obj}")
                dataset_files.delete()
                dataset_obj.delete()
                return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)


class DatasetV2ViewSetOps(GenericViewSet):
    """
    A viewset for performing operations on datasets with Excel files.

    This viewset supports the following actions:

    - `dataset_names`: Returns the names of all datasets that have at least one Excel file.
    - `dataset_file_names`: Given two dataset names, returns the names of all Excel files associated with each dataset.
    - `dataset_col_names`: Given the paths to two Excel files, returns the column names of each file as a response.
    - `dataset_join_on_columns`: Given the paths to two Excel files and the names of two columns, returns a JSON response with the result of an inner join operation on the two files based on the selected columns.
    """

    serializer_class = DatasetV2Serializer
    queryset = DatasetV2.objects.all()
    pagination_class = CustomPagination

    @action(detail=False, methods=["get"])
    def datasets_names(self, request, *args, **kwargs):
        try:
            datasets_with_excel_files = (
                DatasetV2.objects.prefetch_related("datasets")
                .select_related("user_map")
                .filter(
                    Q(datasets__file__endswith=".xls")
                    | Q(datasets__file__endswith=".xlsx")
                    | Q(datasets__file__endswith=".csv")
                )
                .filter(user_map__organization_id=request.GET.get("org_id"), is_temp=False)
                .distinct()
                .values("name", "id", org_name=F("user_map__organization__name"))
            )
            return Response(datasets_with_excel_files, status=status.HTTP_200_OK)
        except Exception as e:
            error_message = f"An error occurred while fetching dataset names: {e}"
            return Response({"error": error_message}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["post"])
    def datasets_file_names(self, request, *args, **kwargs):
        dataset_ids = request.data.get("datasets")
        user_map = request.data.get("user_map")
        if dataset_ids:
            try:
                # Get list of files for each dataset
                files = (
                    DatasetV2File.objects.select_related("dataset_v2_file", "dataset")
                    .filter(dataset_id__in=dataset_ids)
                    .filter(Q(file__endswith=".xls") | Q(file__endswith=".xlsx") | Q(file__endswith=".csv"))
                    .filter(
                        Q(accessibility__in=["public", "registered"])
                        | Q(dataset__user_map_id=user_map)
                        | Q(dataset_v2_file__approval_status="approved")
                    )
                    .values(
                        "id",
                        "dataset",
                        "standardised_file",
                        dataset_name=F("dataset__name"),
                    )
                    .distinct()
                )
                files = [
                    {
                        **row,
                        "file_name": row.get("standardised_file", "").split("/")[-1],
                    }
                    for row in files
                ]
                return Response(files, status=status.HTTP_200_OK)

            except Exception as e:
                return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response([], status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    def datasets_col_names(self, request, *args, **kwargs):
        try:
            file_paths = request.data.get("files")
            result = {}
            for file_path in file_paths:
                path = file_path
                file_path = unquote(file_path).replace("/media/", "")
                if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                    df = pd.read_excel(
                        os.path.join(settings.DATASET_FILES_URL, file_path),
                        index_col=None,
                        nrows=3,
                    )
                else:
                    df = pd.read_csv(
                        os.path.join(settings.DATASET_FILES_URL, file_path),
                        index_col=False,
                        nrows=3,
                    )
                df = df.drop(df.filter(regex="Unnamed").columns, axis=1)
                result[path] = df.columns.tolist()
                result[Constants.ID] = DatasetV2File.objects.get(standardised_file=file_path).id
            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    def datasets_join_condition(self, request, *args, **kwargs):
        try:
            file_path1 = request.data.get("file_path1")
            file_path2 = request.data.get("file_path2")
            columns1 = request.data.get("columns1")
            columns2 = request.data.get("columns2")
            condition = request.data.get("condition")

            # Load the files into dataframes
            if file_path1.endswith(".xlsx") or file_path1.endswith(".xls"):
                df1 = pd.read_excel(os.path.join(settings.MEDIA_ROOT, file_path1), usecols=columns1)
            else:
                df1 = pd.read_csv(os.path.join(settings.MEDIA_ROOT, file_path1), usecols=columns1)
            if file_path2.endswith(".xlsx") or file_path2.endswith(".xls"):
                df2 = pd.read_excel(os.path.join(settings.MEDIA_ROOT, file_path2), usecols=columns2)
            else:
                df2 = pd.read_csv(os.path.join(settings.MEDIA_ROOT, file_path2), usecols=columns2)
            # Join the dataframes
            result = pd.merge(
                df1,
                df2,
                how=request.data.get("how", "left"),
                left_on=request.data.get("left_on"),
                right_on=request.data.get("right_on"),
            )

            # Return the joined dataframe as JSON
            return Response(result.to_json(orient="records", index=False), status=status.HTTP_200_OK)

        except Exception as e:
            logging.error(str(e), exc_info=True)
            return Response({"error": str(e)}, status=500)

    @action(detail=False, methods=["get"])
    def organization(self, request, *args, **kwargs):
        """GET method: query the list of Organization objects"""
        on_boarded_by = request.GET.get("on_boarded_by", "")
        user_id = request.GET.get("user_id", "")
        try:
            user_org_queryset = (
                UserOrganizationMap.objects.prefetch_related("user_org_map")
                .select_related("organization", "user")
                .annotate(dataset_count=Count("user_org_map__id"))
                .values(
                    name=F("organization__name"),
                    org_id=F("organization_id"),
                    org_description=F("organization__org_description"),
                )
                .filter(user__status=True, dataset_count__gt=0)
                .all()
            )
            if on_boarded_by:
                user_org_queryset = user_org_queryset.filter(
                    Q(user__on_boarded_by=on_boarded_by) | Q(user_id=on_boarded_by)
                )
            else:
                user_onboarded_by = User.objects.get(id=user_id).on_boarded_by
                if user_onboarded_by:
                    user_org_queryset = user_org_queryset.filter(
                        Q(user__on_boarded_by=user_onboarded_by.id) | Q(user__id=user_onboarded_by.id)
                    )
                else:
                    user_org_queryset = user_org_queryset.filter(user__on_boarded_by=None).exclude(user__role_id=6)
            return Response(user_org_queryset, 200)
        except Exception as e:
            error_message = f"An error occurred while fetching Organization details: {e}"
            return Response({"error": error_message}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class StandardisationTemplateView(GenericViewSet):
    serializer_class = StandardisationTemplateViewSerializer
    queryset = StandardisationTemplate.objects.all()

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        LOGGER.info("Standardisation Template Created Successfully.")
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=["put"])
    def update_standardisation_template(self, request, *args, **kwargs):
        update_list = list()
        create_list = list()
        try:
            for data in request.data:
                if data.get(Constants.ID, None):
                    # Update
                    id = data.pop(Constants.ID)
                    instance = StandardisationTemplate.objects.get(id=id)
                    serializer = StandardisationTemplateUpdateSerializer(instance, data=data, partial=True)
                    serializer.is_valid(raise_exception=True)
                    update_list.append(StandardisationTemplate(id=id, **data))
                else:
                    # Create
                    create_list.append(data)

            create_serializer = self.get_serializer(data=create_list, many=True)
            create_serializer.is_valid(raise_exception=True)
            StandardisationTemplate.objects.bulk_update(
                update_list, fields=["datapoint_category", "datapoint_attributes"]
            )
            create_serializer.save()
            return Response(status=status.HTTP_201_CREATED)
        except Exception as error:
            LOGGER.error("Issue while Updating Standardisation Template", exc_info=True)
            return Response(
                f"Issue while Updating Standardisation Template {error}",
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        instance.delete()
        LOGGER.info(f"Deleted datapoint Category from standardisation template {instance.datapoint_category}")
        return Response(status=status.HTTP_204_NO_CONTENT)

    def list(self, request, *args, **kwargs):
        serializer = self.get_serializer(self.get_queryset(), many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class PolicyListAPIView(generics.ListCreateAPIView):
    queryset = Policy.objects.all()
    serializer_class = PolicySerializer


class PolicyDetailAPIView(generics.RetrieveUpdateDestroyAPIView):
    queryset = Policy.objects.all()
    serializer_class = PolicySerializer


class DatasetV2View(GenericViewSet):
    queryset = DatasetV2.objects.all()
    serializer_class = DatasetV2NewListSerializer
    pagination_class = CustomPagination

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        LOGGER.info("Dataset created Successfully.")
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        serializer = DatasetV2DetailNewSerializer(instance=self.get_object())
        return Response(serializer.data, status=status.HTTP_200_OK)

    @authenticate_user(model=DatasetV2)
    def update(self, request, *args, **kwargs):
        # setattr(request.data, "_mutable", True)
        try:
            instance = self.get_object()
            data = request.data
            data["is_temp"] = False
            serializer = self.get_serializer(instance, data=data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    @authenticate_user(model=DatasetV2)
    def destroy(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            instance.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    def requested_datasets(self, request, *args, **kwargs):
        try:
            user_map_id = request.data.get("user_map")
            policy_type = request.data.get("type", None)
            if policy_type == "api":
                dataset_file_id = request.data.get("dataset_file")
                requested_recieved = (
                    UsagePolicy.objects.select_related(
                        "dataset_file",
                        "dataset_file__dataset",
                        "user_organization_map__organization",
                    )
                    .filter(dataset_file__dataset__user_map_id=user_map_id, dataset_file_id=dataset_file_id)
                    .values(
                        "id",
                        "approval_status",
                        "accessibility_time",
                        "updated_at",
                        "created_at",
                        dataset_id=F("dataset_file__dataset_id"),
                        dataset_name=F("dataset_file__dataset__name"),
                        file_name=F("dataset_file__file"),
                        organization_name=F("user_organization_map__organization__name"),
                        organization_email=F("user_organization_map__organization__org_email"),
                        organization_phone_number=F("user_organization_map__organization__phone_number"),
                    )
                    .order_by("-updated_at")
                )
                response_data = []
                for values in requested_recieved:
                    org = {
                        "org_email": values["organization_email"],
                        "name": values["organization_name"],
                        "phone_number": values["organization_phone_number"],
                    }
                    values.pop("organization_email")
                    values.pop("organization_name")
                    values.pop("organization_phone_number")
                    values["file_name"] = values.get("file_name", "").split("/")[-1]

                    values["organization"] = org
                    response_data.append(values)
                return Response(
                    {
                        "recieved": response_data,
                    },
                    200,
                )
            requested_sent = (
                UsagePolicy.objects.select_related(
                    "dataset_file",
                    "dataset_file__dataset",
                    "user_organization_map__organization",
                )
                .filter(user_organization_map=user_map_id)
                .values(
                    "approval_status",
                    "updated_at",
                    "accessibility_time",
                    "type",
                    dataset_id=F("dataset_file__dataset_id"),
                    dataset_name=F("dataset_file__dataset__name"),
                    file_name=F("dataset_file__file"),
                    organization_name=F("dataset_file__dataset__user_map__organization__name"),
                    organization_email=F("dataset_file__dataset__user_map__organization__org_email"),
                )
                .order_by("-updated_at")
            )

            requested_recieved = (
                UsagePolicy.objects.select_related(
                    "dataset_file",
                    "dataset_file__dataset",
                    "user_organization_map__organization",
                )
                .filter(dataset_file__dataset__user_map_id=user_map_id)
                .values(
                    "id",
                    "approval_status",
                    "accessibility_time",
                    "updated_at",
                    "type",
                    dataset_id=F("dataset_file__dataset_id"),
                    dataset_name=F("dataset_file__dataset__name"),
                    file_name=F("dataset_file__file"),
                    organization_name=F("user_organization_map__organization__name"),
                    organization_email=F("user_organization_map__organization__org_email"),
                )
                .order_by("-updated_at")
            )
            return Response(
                {
                    "sent": [
                        {
                            **values,
                            "file_name": values.get("file_name", "").split("/")[-1],
                        }
                        for values in requested_sent
                    ],
                    "recieved": [
                        {
                            **values,
                            "file_name": values.get("file_name", "").split("/")[-1],
                        }
                        for values in requested_recieved
                    ],
                },
                200,
            )
        except Exception as error:
            LOGGER.error("Issue while Retrive requeted data", exc_info=True)
            return Response(
                f"Issue while Retrive requeted data {error}",
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    # def list(self, request, *args, **kwargs):
    #     page = self.paginate_queryset(self.queryset)
    #     serializer = self.get_serializer(page, many=True).exclude(is_temp = True)
    #     return self.get_paginated_response(serializer.data)

    @action(detail=True, methods=["get"])
    def get_dashboard_chart_data(self, request, pk, *args, **kwargs):
        try:
            cols_to_read = [
                " Gender",
                " Constituency",
                " County",
                " Sub County",
                " Crop Production",
                " Livestock Production",
                " Ducks",
                " Other Sheep",
                " Total Area Irrigation",
                " Family",
                " NPK",
                " Superphosphate",
                " CAN",
                " Urea",
                " Other",
                " Do you insure your crops?",
                " Do you insure your farm buildings and other assets?",
                " Other Dual Cattle",
                " Cross breed Cattle",
                " Cattle boma",
                " Small East African Goats",
                " Somali Goat",
                " Other Goat",
                " Chicken -Indigenous",
                " Chicken -Broilers",
                " Chicken -Layers",
            ]

            livestock_columns = ["Other Dual Cattle", "Cross breed Cattle", "Cattle boma"]
            dataset_file_object = DatasetV2File.objects.get(id=pk)
            dataset_file = str(dataset_file_object.standardised_file)
            print(dataset_file)
            try:
                if dataset_file.endswith(".xlsx") or dataset_file.endswith(".xls"):
                    df = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, dataset_file))
                elif dataset_file.endswith(".csv"):
                    df = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, dataset_file), usecols=cols_to_read)
                    df.columns = df.columns.str.strip()

                else:
                    return Response(
                        "Unsupported file please use .xls or .csv.",
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                df["Ducks"] = pd.to_numeric(df["Ducks"], errors="coerce")
                df["Other Sheep"] = pd.to_numeric(df["Other Sheep"], errors="coerce")
                df["Family"] = pd.to_numeric(df["Family"], errors="coerce")
                df["Total Area Irrigation"] = pd.to_numeric(df["Total Area Irrigation"], errors="coerce")
                df["NPK"] = pd.to_numeric(df["NPK"], errors="coerce")
                df["Superphosphate"] = pd.to_numeric(df["Superphosphate"], errors="coerce")
                df["CAN"] = pd.to_numeric(df["CAN"], errors="coerce")
                df["Urea"] = pd.to_numeric(df["Urea"], errors="coerce")
                df["Other"] = pd.to_numeric(df["Other"], errors="coerce")

                df["Other Dual Cattle"] = pd.to_numeric(df["Other Dual Cattle"], errors="coerce")
                df["Cross breed Cattle"] = pd.to_numeric(df["Cross breed Cattle"], errors="coerce")
                df["Cattle boma"] = pd.to_numeric(df["Cattle boma"], errors="coerce")
                df["Small East African Goats"] = pd.to_numeric(df["Small East African Goats"], errors="coerce")
                df["Somali Goat"] = pd.to_numeric(df["Somali Goat"], errors="coerce")
                df["Other Goat"] = pd.to_numeric(df["Other Goat"], errors="coerce")
                df["Chicken -Indigenous"] = pd.to_numeric(df["Chicken -Indigenous"], errors="coerce")
                df["Chicken -Broilers"] = pd.to_numeric(df["Chicken -Broilers"], errors="coerce")
                df["Chicken -Layers"] = pd.to_numeric(df["Chicken -Layers"], errors="coerce")

                df["Do you insure your crops?"] = pd.to_numeric(df["Do you insure your crops?"], errors="coerce")
                df["Do you insure your farm buildings and other assets?"] = pd.to_numeric(
                    df["Do you insure your farm buildings and other assets?"], errors="coerce"
                )

                obj = {
                    "total_no_of_records": len(df),
                    "male_count": np.sum(df["Gender"] == 1),
                    "female_count": np.sum(df["Gender"] == 2),
                    "constituencies": np.unique(df["Constituency"]).size,
                    "counties": np.unique(df["County"]).size,
                    "sub_counties": np.unique(df["Sub County"]).size,
                    "farming_practices": {
                        "crop_production": np.sum(df["Crop Production"] == 1),
                        "livestock_production": np.sum(df["Livestock Production"] == 1),
                    },
                    "livestock_and_poultry_production": {
                        "cows": int(
                            (df[["Other Dual Cattle", "Cross breed Cattle", "Cattle boma"]]).sum(axis=1).sum()
                        ),
                        "goats": int(df[["Small East African Goats", "Somali Goat", "Other Goat"]].sum(axis=1).sum()),
                        "chickens": int(
                            df[["Chicken -Indigenous", "Chicken -Broilers", "Chicken -Layers"]].sum(axis=1).sum()
                        ),
                        "ducks": int(np.sum(df["Ducks"])),
                        "sheep": int(np.sum(df["Other Sheep"])),
                    },
                    "financial_livelihood": {
                        "lenders": 0,
                        "relatives": int(np.sum(df["Family"])),
                        "traders": 0,
                        "agents": 0,
                        "institutional": 0,
                    },
                    "water_sources": {
                        "borewell": 0,
                        "irrigation": int(np.sum(df["Total Area Irrigation"])),
                        "rainwater": 0,
                    },
                    "insurance_information": {
                        "insured_crops": int(np.sum(df["Do you insure your crops?"])),
                        "insured_machinery": int(np.sum(df["Do you insure your farm buildings and other assets?"])),
                    },
                    "popular_fertilizer_user": {
                        "npk": int(np.sum(df["NPK"])),
                        "ssp": int(np.sum(df["Superphosphate"])),
                        "can": int(np.sum(df["CAN"])),
                        "urea": int(np.sum(df["Urea"])),
                        "Others": int(np.sum(df["Other"])),
                    },
                }

            except Exception as e:
                print(e)
                return Response(
                    "Something went wrong, please try again.",
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            return Response(
                obj,
                status=status.HTTP_200_OK,
            )

        except DatasetV2File.DoesNotExist:
            return Response(
                "No dataset file for the provided id.",
                status=status.HTTP_404_NOT_FOUND,
            )
    

class DatasetFileV2View(GenericViewSet):
    queryset = DatasetV2File.objects.all()
    serializer_class = DatasetFileV2NewSerializer

    def create(self, request, *args, **kwargs):
        validity = check_file_name_length(
            incoming_file_name=request.data.get("file"), accepted_file_name_size=NumericalConstants.FILE_NAME_LENGTH
        )
        if not validity:
            return Response(
                {"message": f"File name should not be more than {NumericalConstants.FILE_NAME_LENGTH} characters."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        data = serializer.data
        instance = DatasetV2File.objects.get(id=data.get("id"))
        instance.standardised_file = instance.file  # type: ignore
        instance.file_size = os.path.getsize(os.path.join(settings.DATASET_FILES_URL, str(instance.file)))
        instance.save()
        LOGGER.info("Dataset created Successfully.")
        data = DatasetFileV2NewSerializer(instance)
        return Response(data.data, status=status.HTTP_201_CREATED)

    @authenticate_user(model=DatasetV2File)
    def update(self, request, *args, **kwargs):
        # setattr(request.data, "_mutable", True)
        try:
            data = request.data
            instance = self.get_object()
            # Generate the file and write the path to standardised file.
            standardised_configuration = request.data.get("standardised_configuration")
            mask_columns = request.data.get(
                "mask_columns",
            )
            config = request.data.get("config")
            file_path = str(instance.file)

            if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
                df = pd.read_excel(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=None)
            else:
                df = pd.read_csv(os.path.join(settings.DATASET_FILES_URL, file_path), index_col=False)

            df[mask_columns] = "######"

            df.rename(columns=standardised_configuration, inplace=True)
            df.columns = df.columns.astype(str)
            df = df.drop(df.filter(regex="Unnamed").columns, axis=1)

            if not os.path.exists(os.path.join(settings.DATASET_FILES_URL, instance.dataset.name, instance.source)):
                os.makedirs(os.path.join(settings.DATASET_FILES_URL, instance.dataset.name, instance.source))

            file_name = os.path.basename(file_path).replace(".", "_standerdise.")
            if file_path.endswith(".csv"):
                df.to_csv(
                    os.path.join(
                        settings.DATASET_FILES_URL,
                        instance.dataset.name,
                        instance.source,
                        file_name,
                    )
                )  # type: ignore
            else:
                df.to_excel(
                    os.path.join(
                        settings.DATASET_FILES_URL,
                        instance.dataset.name,
                        instance.source,
                        file_name,
                    )
                )  # type: ignore

            # data = request.data
            standardised_file_path = os.path.join(instance.dataset.name, instance.source, file_name)
            data["standardised_configuration"] = config
            data["file_size"] = os.path.getsize(os.path.join(settings.DATASET_FILES_URL, str(standardised_file_path)))
            serializer = self.get_serializer(instance, data=data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            DatasetV2File.objects.filter(id=serializer.data.get("id")).update(standardised_file=standardised_file_path)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    def list(self, request, *args, **kwargs):
        data = DatasetV2File.objects.filter(dataset=request.GET.get("dataset")).values("id", "file")
        return Response(data, status=status.HTTP_200_OK)

    @authenticate_user(model=DatasetV2File)
    def destroy(self, request, *args, **kwargs):
        try:
            dataset_file = self.get_object()
            dataset_file.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)

    # @action(detail=False, methods=["put"])
    @authenticate_user(model=DatasetV2File)
    def patch(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = self.get_serializer(instance, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_400_BAD_REQUEST)


class UsagePolicyListCreateView(generics.ListCreateAPIView):
    queryset = UsagePolicy.objects.all()
    serializer_class = UsagePolicySerializer


class UsagePolicyRetrieveUpdateDestroyView(generics.RetrieveUpdateDestroyAPIView):
    queryset = UsagePolicy.objects.all()
    serializer_class = UsagePolicySerializer
    api_builder_serializer_class = APIBuilderSerializer

    def patch(self, request, *args, **kwargs):
        instance = self.get_object()
        approval_status = request.data.get("approval_status")
        policy_type = request.data.get("type", None)
        instance.api_key = None
        try:
            if policy_type == "api":
                if approval_status == "approved":
                    instance.api_key = generate_api_key()
            serializer = self.api_builder_serializer_class(instance, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            return Response(serializer.data, status=200)

        except ValidationError as e:
            LOGGER.error(e, exc_info=True)
            return Response(e.detail, status=status.HTTP_400_BAD_REQUEST)

        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response(str(error), status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DatahubNewDashboard(GenericViewSet):
    """Datahub Dashboard viewset"""

    pagination_class = CustomPagination

    def participant_metics(self, data):
        on_boarded_by = data.get("onboarded_by")
        role_id = data.get("role_id")
        user_id = data.get("user_id")
        result = {}
        try:
            if on_boarded_by != "None" or role_id == str(6):
                result["participants_count"] = (
                    UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                    .filter(
                        user__status=True,
                        user__on_boarded_by=on_boarded_by if on_boarded_by != "None" else user_id,
                        user__role=3,
                        user__approval_status=True,
                    )
                    .count()
                )
            elif role_id == str(1):
                result["co_steward_count"] = (
                    UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                    .filter(user__status=True, user__role=6)
                    .count()
                )
                result["participants_count"] = (
                    UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                    .filter(
                        user__status=True,
                        user__role=3,
                        user__on_boarded_by=None,
                        user__approval_status=True,
                    )
                    .count()
                )
            else:
                result["participants_count"] = (
                    UserOrganizationMap.objects.select_related(Constants.USER, Constants.ORGANIZATION)
                    .filter(
                        user__status=True,
                        user__role=3,
                        user__on_boarded_by=None,
                        user__approval_status=True,
                    )
                    .count()
                )
            logging.info("Participants Metrics completed")
            return result
        except Exception as error:  # type: ignore
            logging.error(
                "Error while filtering the participants. ERROR: %s",
                error,
                exc_info=True,
            )
            raise Exception(str(error))

    def dataset_metrics(self, data, request):
        on_boarded_by = data.get("onboarded_by")
        role_id = data.get("role_id")
        user_id = data.get("user_id")
        user_org_map = data.get("map_id")
        try:
            query = (
                DatasetV2.objects.prefetch_related("datasets")
                .select_related(
                    Constants.USER_MAP,
                    Constants.USER_MAP_USER,
                    Constants.USER_MAP_ORGANIZATION,
                )
                .exclude(is_temp=True)
            )
            if on_boarded_by != "None" or role_id == str(6):
                query = query.filter(
                    Q(user_map__user__on_boarded_by=on_boarded_by if on_boarded_by != "None" else user_id)
                    | Q(user_map__user_id=on_boarded_by if on_boarded_by != "None" else user_id)
                )
            else:
                query = query.filter(user_map__user__on_boarded_by=None).exclude(user_map__user__role_id=6)
            logging.info("Datasets Metrics completed")
            return query
        except Exception as error:  # type: ignore
            logging.error("Error while filtering the datasets. ERROR: %s", error, exc_info=True)
            raise Exception(str(error))

    def connector_metrics(self, data, dataset_query, request):
        # on_boarded_by = data.get("onboarded_by")
        # role_id = data.get("role_id")
        user_id = data.get("user_id")
        user_org_map = data.get("map_id")
        my_dataset_used_in_connectors = (
            dataset_query.prefetch_related("datasets__right_dataset_file")
            .values("datasets__right_dataset_file")
            .filter(datasets__right_dataset_file__connectors__user_id=user_id)
            .distinct()
            .count()
            + dataset_query.prefetch_related("datasets__left_dataset_file")
            .values("datasets__left_dataset_file")
            .filter(datasets__left_dataset_file__connectors__user_id=user_id)
            .distinct()
            .count()
        )
        connectors_query = Connectors.objects.filter(user_id=user_id).all()

        other_datasets_used_in_my_connectors = (
            dataset_query.prefetch_related("datasets__right_dataset_file")
            .select_related("datasets__right_dataset_file__connectors")
            .filter(datasets__right_dataset_file__connectors__user_id=user_id)
            .values("datasets__right_dataset_file")
            .exclude(user_map_id=user_org_map)
            .distinct()
            .count()
        ) + (
            dataset_query.prefetch_related("datasets__left_dataset_file")
            .select_related("datasets__left_dataset_file__connectors")
            .filter(datasets__left_dataset_file__connectors__user_id=user_id)
            .values("datasets__left_dataset_file")
            .exclude(user_map_id=user_org_map)
            .distinct()
            .count()
        )
        return {
            "total_connectors_count": connectors_query.count(),
            "other_datasets_used_in_my_connectors": other_datasets_used_in_my_connectors,
            "my_dataset_used_in_connectors": my_dataset_used_in_connectors,
            "recent_connectors": ConnectorsListSerializer(
                connectors_query.order_by("-updated_at")[0:3], many=True
            ).data,
        }

    @action(detail=False, methods=["get"])
    @http_request_mutation
    def dashboard(self, request):
        """Retrieve datahub dashboard details"""
        data = request.META
        try:
            participant_metrics = self.participant_metics(data)
            dataset_query = self.dataset_metrics(data, request)
            # This will fetch connectors metrics
            connector_metrics = self.connector_metrics(data, dataset_query, request)
            if request.GET.get("my_org", False):
                dataset_query = dataset_query.filter(user_map_id=data.get("map_id"))
            dataset_file_metrics = (
                dataset_query.values("datasets__source")
                .annotate(
                    dataset_count=Count("id", distinct=True),
                    file_count=Count("datasets__file", distinct=True),
                    total_size=Sum("datasets__file_size"),
                )
                .filter(file_count__gt=0)
            )

            dataset_state_metrics = dataset_query.values(state_name=F("geography__state__name")).annotate(
                dataset_count=Count("id", distinct=True)
            )
            distinct_keys = (
                DatasetV2.objects.annotate(
                    key=Func(
                        "category",
                        function="JSONB_OBJECT_KEYS",
                        output_field=CharField(),
                    )
                )
                .values_list("key", flat=True)
                .distinct()
            )

            # Iterate over the distinct keys and find the count for each key
            dataset_category_metrics = {}
            for key in distinct_keys:
                dataset_count = dataset_query.filter(category__has_key=key).count()
                if dataset_count:
                    dataset_category_metrics[key] = dataset_count
            recent_datasets = DatasetV2ListNewSerializer(dataset_query.order_by("-updated_at")[0:3], many=True).data
            data = {
                "user": UserOrganizationMap.objects.select_related("user", "organization")
                .filter(id=data.get("map_id"))
                .values(
                    first_name=F("user__first_name"),
                    last_name=F("user__last_name"),
                    logo=Concat(
                        Value("media/"),
                        F("organization__logo"),
                        output_field=CharField(),
                    ),
                    org_email=F("organization__org_email"),
                    name=F("organization__name"),
                )
                .first(),
                "total_participants": participant_metrics,
                "dataset_file_metrics": dataset_file_metrics,
                "dataset_state_metrics": dataset_state_metrics,
                "total_dataset_count": dataset_query.count(),
                "dataset_category_metrics": dataset_category_metrics,
                "recent_datasets": recent_datasets,
                **connector_metrics,
            }
            return Response(data, status=status.HTTP_200_OK)

        except Exception as error:
            LOGGER.error(error, exc_info=True)
            return Response({}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# @http_request_mutation
class ResourceManagementViewSet(GenericViewSet):
    """
    Resource Management viewset.
    """

    queryset = Resource.objects.all()
    serializer_class = ResourceSerializer
    pagination_class = CustomPagination

    @http_request_mutation
    def create(self, request, *args, **kwargs):
        try:
            user_map = request.META.get("map_id")
            request.data._mutable = True
            request.data["user_map"] = user_map

            serializer = self.get_serializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            serializer.save()

            return Response(serializer.data, status=status.HTTP_201_CREATED)
        except ValidationError as e:
            return Response(e.detail, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            LOGGER.error(e)
            return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def update(self, request, *args, **kwargs):
        try:
            instance = self.get_object()
            serializer = self.get_serializer(instance, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            return Response(status=status.HTTP_200_OK)
        except ValidationError as e:
            return Response(e.detail, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            LOGGER.error(e)
            return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @http_request_mutation
    def list(self, request, *args, **kwargs):
        try:
            user_map = request.META.get("map_id")
            # import pdb; pdb.set_trace();
            if request.GET.get("others", None):
                queryset = Resource.objects.exclude(user_map=user_map)
            else:
                queryset = Resource.objects.filter(user_map=user_map)
                # Created by me.

            page = self.paginate_queryset(queryset)
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        except ValidationError as e:
            return Response(e.detail, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            LOGGER.error(e)
            return Response(str(e), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def destroy(self, request, *args, **kwargs):
        resource = self.get_object()
        resource.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    def retrieve(self, request, *args, **kwargs):
        resource = self.get_object()
        serializer = self.get_serializer(resource)
        # serializer.is_valid(raise_exception=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class ResourceFileManagementViewSet(GenericViewSet):
    """
    Resource File Management
    """

    queryset = ResourceFile.objects.all()
    serializer_class = ResourceFileSerializer

    @http_request_mutation
    def create(self, request, *args, **kwargs):
        request.data._mutable = True
        request.data["file_size"] = request.FILES.get("file").size
        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        instance.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
