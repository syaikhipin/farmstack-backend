from participant.models import SupportTicketV2


class SupportTicketServices:
    @classmethod
    def get_support_tickets_service(cls, map_id: str, status: str , start_date: str ,end_date: str
                                    , category: str
                                    ):
        queryset = SupportTicketV2.objects.filter(
            user_map=map_id
        ).select_related("user_map", "user_map__organization", "user_map__user").order_by("-created_at")

        if status:
            queryset = queryset.filter(status=status)

        if start_date and end_date:
            queryset = queryset.filter(created_at__date__range=[start_date,end_date])

        if category:
            queryset = queryset.filter(category=category)

        if status:
            queryset = queryset.filter(status=status)


        return queryset
