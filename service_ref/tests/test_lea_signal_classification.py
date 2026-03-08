from __future__ import annotations

from service_ref import build_service_referential as legacy


KNOWN_CITIES = {
    legacy._normalize_city("AMIENS"),
    legacy._normalize_city("PIERREFONDS"),
    legacy._normalize_city("BEAUVAIS"),
    legacy._normalize_city("CHOISY AU BAC"),
    legacy._normalize_city("LAMORLAYE"),
    legacy._normalize_city("ST JUST EN CHAUSSEE"),
}


def _classify(value: str) -> dict[str, object]:
    payload = legacy.classify_lea_signal(value, "CMD - Secteur géographique2", KNOWN_CITIES)
    assert payload is not None
    return payload


def test_classifies_precise_address() -> None:
    payload = _classify("43 Avenue d'Italie 80000 Amiens")
    assert payload["signal_kind"] == "postal_address_precise"


def test_classifies_mixed_site_address() -> None:
    payload = _classify("VALEO FRANCE Rue de la Cavée du Château 60240 Reilly")
    assert payload["signal_kind"] == "mixed_site_address"


def test_classifies_technical_anchor() -> None:
    payload = _classify("CHAMBRE TELOISE N-1 ST JEAN AUX BOIS")
    assert payload["signal_kind"] == "technical_site_anchor"


def test_classifies_business_site_label() -> None:
    payload = _classify("MAIRIE DE CHOISY AU BAC")
    assert payload["signal_kind"] == "site_label_business"


def test_classifies_noise() -> None:
    payload = _classify("PASS1 et 2")
    assert payload["signal_kind"] == "noise"
