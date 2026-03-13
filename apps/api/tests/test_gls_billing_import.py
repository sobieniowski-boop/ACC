from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from openpyxl import Workbook

from app.services.gls_billing_import import (
    ImportedGLSShipmentSeed,
    LocalPackageCandidate,
    _discover_gls_correction_xlsx_files,
    _load_bl_order_candidates,
    _collect_seed_links,
    _resolve_seed_parcel_numbers,
    _should_skip_import_file,
    parse_gls_billing_csv,
    parse_gls_billing_correction_xlsx,
    parse_gls_bl_map_xlsx,
)
from app.services.bl_order_lookup import ResolvedBlOrder


def test_parse_gls_billing_csv_reads_cost_rows(tmp_path):
    path = tmp_path / "GLS_6501031953.csv"
    path.write_text(
        '"invoice_num";"date";"delivery_date_x";"parcel_num";"rname1";"rpost";"rcity";"rcountry";"weight";"weight_declared";"weight_billing";"netto";"toll";"fuel_surcharge";"storewarehouse_price";"surcharge";"billing_type";"note1";"dim_combined";"weight_volumetric";"parcel_status";"srv"\n'
        '"6501031953";"2026-01-08";"2026-02-09";"30768067838";"NETFOX";"32-400";"MYSLENICE";"PL";"3,06";"9,00";"11,89";"21,81";"0,98";"3,2715";"0,00";"";"V";"139482725";"137.00x62.00x7.00";"11,89";"Doreczone";"SRS"\n',
        encoding="utf-8",
    )

    rows = parse_gls_billing_csv(path)

    assert len(rows) == 1
    assert rows[0]["document_number"] == "6501031953"
    assert rows[0]["parcel_number"] == "30768067838"
    assert rows[0]["note1"] == "139482725"
    assert rows[0]["net_amount"] == 21.81
    assert rows[0]["billing_period"] is None


def test_parse_gls_bl_map_xlsx_reads_tracking_to_order_map(tmp_path):
    path = tmp_path / "GLS - BL.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "map"
    ws.append(["tracking_number", "order_id", "custom_1"])
    ws.append([30621292492, 42451050, "Amazon"])
    wb.save(path)

    rows = parse_gls_bl_map_xlsx(path)

    assert len(rows) == 1
    assert rows[0]["tracking_number"] == "30621292492"
    assert rows[0]["bl_order_id"] == 42451050
    assert rows[0]["map_source"] == "Amazon"


def test_parse_gls_billing_correction_xlsx_reads_corrected_rows(tmp_path):
    path = tmp_path / "Korekty kosztowe" / "12.2025" / "Specyfikacja Netfox 6500900532.xlsx"
    path.parent.mkdir(parents=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Arkusz1"
    ws.append(
        [
            "Dok. fakturowania",
            "Data wystawienia faktury",
            "Data sprzedaży",
            "Zleceniodawca",
            "Waluta dokumentu",
            "Materiał",
            "Nr paczki",
            "Reklamowane",
            "Opis",
            "Nr przesyłki",
            "Referencja klienta",
            "Adresu odbioru",
            "Nazwa adresu adbioru",
            "Adres adresu odbiory",
            "Kod pocztowy odbiorcy",
            "Miejscowość adresu odbioru",
            "Kraj",
            "Data nadania",
            "Data doręczenia",
            "Odbiorca",
            "Dane adresowe",
            "Kod pocztowy",
            "Miejscowość",
            "Kraj",
            "Ilość",
            "Cena jednostkowa",
            "Waga [kg]",
            "Wartość netto",
            "Prawidłowa kwota",
            "Różnica",
            "Stawka VAT",
            "Stawka dopłaty paliwowej",
            "Dopłata drogowaj",
            "Różnica korekty paliwowej",
            "Objaśnienie stawki VAT",
        ]
    )
    ws.append(
        [
            "6500900532",
            "2025-11-18",
            "2025-11-09",
            "6160133810",
            "PLN",
            "PRD_EBP_PARC",
            "30645415621",
            None,
            "Usługi kurierskie",
            "306454156218",
            None,
            None,
            None,
            None,
            "28902",
            "Getafe",
            "ES",
            "2025-10-24",
            "2025-11-05",
            "ALBA GARCIA DE LA GAMA",
            "Calle Capellanes 9",
            "28902",
            "Getafe",
            "ES",
            1,
            65.51,
            26,
            65.51,
            22.74,
            42.77,
            0.23,
            "15.00%",
            0.98,
            6.4155,
            None,
        ]
    )
    wb.save(path)

    rows = parse_gls_billing_correction_xlsx(path)

    assert len(rows) == 1
    assert rows[0]["document_number"] == "6500900532"
    assert rows[0]["billing_period"] == "2025.12"
    assert rows[0]["parcel_number"] == "30645415621"
    assert rows[0]["original_net_amount"] == 65.51
    assert rows[0]["corrected_net_amount"] == 22.74
    assert rows[0]["net_delta_amount"] == 42.77
    assert rows[0]["fuel_rate_pct"] == 0.15
    assert rows[0]["toll_amount"] == 0.98
    assert rows[0]["fuel_correction_amount"] == 6.4155


def test_discover_gls_correction_xlsx_files_reads_only_correction_folder(tmp_path):
    correction_file = tmp_path / "Korekty kosztowe" / "12.2025" / "Specyfikacja Netfox 6500900532.xlsx"
    regular_file = tmp_path / "2025.12" / "specyfikacja 6500973189.xlsx"
    correction_file.parent.mkdir(parents=True)
    regular_file.parent.mkdir(parents=True)
    correction_file.write_text("x", encoding="utf-8")
    regular_file.write_text("x", encoding="utf-8")

    files = _discover_gls_correction_xlsx_files(tmp_path)

    assert files == [correction_file.resolve()]


def test_should_skip_import_file_only_when_imported_snapshot_matches():
    mtime = datetime(2026, 3, 6, 17, 0, 0)

    assert (
        _should_skip_import_file(
            (123, mtime, "imported"),
            file_size=123,
            file_mtime=mtime,
            force_reimport=False,
        )
        is True
    )
    assert (
        _should_skip_import_file(
            (123, mtime, "failed"),
            file_size=123,
            file_mtime=mtime,
            force_reimport=False,
        )
        is False
    )
    assert (
        _should_skip_import_file(
            (123, mtime, "imported"),
            file_size=123,
            file_mtime=mtime,
            force_reimport=True,
        )
        is False
    )


def test_collect_seed_links_prefers_tracking_match():
    seed = ImportedGLSShipmentSeed(
        parcel_number="30768067838",
        row_date=None,
        delivery_date=None,
        parcel_status="Doreczone",
        service_code="SRS",
        note1="139482725",
        recipient_name="NETFOX",
        recipient_country="PL",
        billing_period="2026.02",
        total_amount=26.0615,
        line_count=1,
    )
    tracking_candidate = LocalPackageCandidate(
        amazon_order_id="ORDER-1",
        acc_order_id="00000000-0000-0000-0000-000000000111",
        bl_order_id=139482725,
        package_order_id=139482725,
        courier_package_nr="30768067838",
        courier_inner_number=None,
    )
    note1_candidate = LocalPackageCandidate(
        amazon_order_id="ORDER-2",
        acc_order_id="00000000-0000-0000-0000-000000000222",
        bl_order_id=139482725,
        package_order_id=None,
        courier_package_nr=None,
        courier_inner_number=None,
    )

    links = _collect_seed_links(
        seed=seed,
        by_tracking={"30768067838": [tracking_candidate]},
        by_tracking_bl_map={},
        by_bl_order={"139482725": [note1_candidate]},
    )

    assert any(link["link_method"] == "billing_parcel_tracking" for link in links)
    assert any(link["is_primary"] for link in links if link["amazon_order_id"] == "ORDER-1")


def test_collect_seed_links_falls_back_to_note1_bl_order():
    seed = ImportedGLSShipmentSeed(
        parcel_number="30768067839",
        row_date=None,
        delivery_date=None,
        parcel_status=None,
        service_code="SRS",
        note1="139482725",
        recipient_name=None,
        recipient_country=None,
        billing_period="2026.02",
        total_amount=10.0,
        line_count=1,
    )
    candidate = LocalPackageCandidate(
        amazon_order_id="ORDER-3",
        acc_order_id="00000000-0000-0000-0000-000000000333",
        bl_order_id=139482725,
        package_order_id=None,
        courier_package_nr=None,
        courier_inner_number=None,
    )

    links = _collect_seed_links(
        seed=seed,
        by_tracking={},
        by_tracking_bl_map={},
        by_bl_order={"139482725": [candidate]},
    )

    assert len(links) == 1
    assert links[0]["link_method"] == "billing_note1_bl_order"
    assert links[0]["is_primary"] is True


def test_resolve_seed_parcel_numbers_uses_changed_scope_by_default():
    result = _resolve_seed_parcel_numbers(
        None,
        changed_parcels={" 30768067838 ", ""},
        seed_all_existing=False,
    )

    assert result == {"30768067838"}


def test_resolve_seed_parcel_numbers_can_load_all_existing():
    with patch(
        "app.services.gls_billing_import._load_all_billing_parcel_numbers",
        return_value={"A", "B"},
    ) as mocked:
        result = _resolve_seed_parcel_numbers(
            object(),
            changed_parcels={"X"},
            seed_all_existing=True,
        )

    assert result == {"A", "B"}
    mocked.assert_called_once()


def test_load_bl_order_candidates_uses_resolved_bl_order_lookup():
    with patch(
        "app.services.gls_billing_import.resolve_bl_orders_to_acc_orders",
        return_value={
            139482725: ResolvedBlOrder(
                bl_order_id=139482725,
                amazon_order_id="ORDER-1",
                acc_order_id="00000000-0000-0000-0000-000000000111",
            )
        },
    ):
        result = _load_bl_order_candidates(object(), bl_order_values=["139482725"])

    assert "139482725" in result
    assert result["139482725"][0].amazon_order_id == "ORDER-1"
