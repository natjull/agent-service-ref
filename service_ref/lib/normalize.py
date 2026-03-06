from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import ROLE_BY_OFFER


norm_text = legacy.norm_text
norm_slug = legacy.norm_slug
clean_business_label = legacy.clean_business_label
business_tokens = legacy.business_tokens
classify_offer = legacy.classify_offer
ROLE_BY_OFFER_NORMALIZED = {norm_text(key): value for key, value in ROLE_BY_OFFER.items()}
