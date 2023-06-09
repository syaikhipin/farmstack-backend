from rest_framework.response import Response

from participant.constants import FilterAPIConstants
from participant.models import SupportTicketV2, STATUS, CATEGORY


class SupportTicketInternalServices:
    @classmethod
    def filter_support_ticket_service(cls, map_id: str, role_id: str, onboarded_by_id: str, org_id: str, status: STATUS,
                                      category: CATEGORY, start_date: str, end_date: str,
                                      results_for: FilterAPIConstants.ticket_visibility):
        queryset = SupportTicketV2.objects.filter(user_map__organization_id=org_id).order_by("-created_at")
        roles_under_me = []
        print("SDFASDF")
        print(results_for)
        if str(role_id) == "1":
            # the person is an admin/steward so he should be able to view tickets:
            # 1. raise by co-stewards
            # 2. raised by participants under the steward.
            roles_under_me = [3, 6]
            queryset = queryset.filter(user_map_id=map_id, user__map__user__on_boarded_by_id=None)

        if str(role_id) == "6":
            # the person is co-steward
            # 1. raised by himself
            # 2. raised by participants under himself.
            roles_under_me = [3, 6]
            queryset = queryset.filter(
                user_map__user__on_boarded_by_id=onboarded_by_id
            )

        if str(role_id) == "3":
            # participant
            # can only see his tickets
            roles_under_me = [3]
            queryset = queryset.filter(
                user_map__user__on_boarded_by_id=onboarded_by_id
            )

        if status:
            queryset = queryset.filter(status=status)

        if category:
            queryset = queryset.filter(category=category)

        if start_date and end_date:
            print("comes here")
            queryset = queryset.filter(

            )

        if results_for == "myself":
            queryset = queryset
        elif results_for == "underme":
            print(roles_under_me)
            queryset = queryset.filter(
                user_map__user__role_id__in=roles_under_me
            )
        else:
            return Response({
                "message": "Invalid Param"
            })

        return queryset
