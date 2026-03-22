from src.publish.dedupe import PORTAL_LABELS, PORTAL_ORDER, portal_label, portal_slug, resolve_listing_identity
from src.publish.history import load_master_records, load_published_summary, list_published_dates
from src.publish.status_store import WORKFLOW_STATUSES, read_status_map


def publish_daily(*args, **kwargs):
    from src.publish.runner import publish_daily as _publish_daily

    return _publish_daily(*args, **kwargs)


def publish_records(*args, **kwargs):
    from src.publish.runner import publish_records as _publish_records

    return _publish_records(*args, **kwargs)


def set_listing_status(*args, **kwargs):
    from src.publish.runner import set_listing_status as _set_listing_status

    return _set_listing_status(*args, **kwargs)


def load_client_view(*args, **kwargs):
    from src.publish.runner import load_client_view as _load_client_view

    return _load_client_view(*args, **kwargs)


__all__ = [
    "PORTAL_LABELS",
    "PORTAL_ORDER",
    "WORKFLOW_STATUSES",
    "load_client_view",
    "load_master_records",
    "load_published_summary",
    "list_published_dates",
    "portal_label",
    "portal_slug",
    "publish_daily",
    "publish_records",
    "read_status_map",
    "resolve_listing_identity",
    "set_listing_status",
]

