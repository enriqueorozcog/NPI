import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qs, urlparse

import oracledb
import openpyxl
import requests

from pmx_auth import PmxAuthError, PmxClient


APP_DIR = Path(__file__).resolve().parent


def _load_dotenv_if_present():

    env_path = APP_DIR / ".env"
    if not env_path.exists():
        return
    with env_path.open("r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


_load_dotenv_if_present()

ORACLE_DSN = "10.235.5.11:1521/pmxesdb1"
SITE_IDENTIFIER = "0001004439"
WARRANTY_URL = "https://ustrl.mp.microsoft.com/v2.0/devices/{serial_number}"
GENEALOGY_URL = "https://api.supplychain.microsoft.com/Manufacturing/GenealogyService/V2/Devices/{serial_number}"
GENEALOGY_CACHE_TTL = timedelta(hours=6)
GENEALOGY_NEGATIVE_CACHE_TTL = timedelta(minutes=10)
OVER_TAT_ITEMS_URL = "http://10.235.4.15:8080/PegApRestful/api/dashboard/tat/over/items"
OVER_TAT_ITEMS_PARAMS = {
    "begin": "2017-01-01",
    "model": "Surface-SUR",
    "days": 0,
}
YIELD_STATIONS_URL = "http://10.235.4.15:8080/PegApRestful/api/dashboard/yield/stations"
YIELD_STATIONS_LOOKBACK_DAYS = 6
YIELD_CACHE_TTL = timedelta(minutes=10)
STATION_SERIALS_URL = "http://10.235.4.15:8080/PegApRestful/api/dashboard/yield/stations/download"
STATION_SERIALS_CACHE_TTL = timedelta(minutes=10)
PACK_CURR_STATUS = "MSRSFSURPACK"
PACK_STATUS = "MSRHANDOVER"
PACK_CACHE_TTL = timedelta(minutes=2)
EXCHANGE_CURR_STATUS = ("MSRSURRPL", "MSRSFFA", "MSRSLOWLANECHECKOUT")
EXCHANGE_STATUS_BY_CURR_STATUS = {
    "MSRSURRPL": "CLOSED",
    "MSRSFFA": "CLOSED",
    "MSRSLOWLANECHECKOUT": "CLOSED",
}
# Units closed out from MSRSLOWLANECHECKOUT have no matching Code List entry, so
# Disposition/Issue are fixed for this scenario per business rule.
LOWLANE_CHECKOUT_DISPOSITION = "URC"
LOWLANE_CHECKOUT_ISSUE = "DAE LAB"
EXCHANGE_CACHE_TTL = timedelta(minutes=2)
# Exception badge/list: Exchange records whose Disposition is URC.
EXCEPTION_DISPOSITION = "URC"
EXCEPTION_CACHE_TTL = timedelta(minutes=2)
# ERS_RMA_INF
SURFACE_PRODUCT_TYPES = ("N", "NL")
CACHE_TTL = timedelta(minutes=10)
WARRANTY_CACHE_TTL = timedelta(hours=6)
WARRANTY_NEGATIVE_CACHE_TTL = timedelta(minutes=10)

WARRANTY_MAX_WORKERS = 4
WARRANTY_TOKEN_CACHE_TTL = timedelta(minutes=30)
DEVICE_DETAILS_CACHE_TTL = timedelta(hours=6)
INVENTORY_WAREHOUSE_LIKE = "%MS_JRZ%"
INVENTORY_CACHE_TTL = timedelta(minutes=5)
LOCATION_CODE_PATTERN = re.compile(r"MS-\d{3,5}")
active_serials_cache = {"records": None, "updated_at": None}
cache_lock = Lock()
yield_cache = {}
yield_cache_lock = Lock()
station_serials_cache = {}
station_serials_cache_lock = Lock()
pack_cache = {}
pack_cache_lock = Lock()
exchange_cache = {}
exchange_cache_lock = Lock()
exception_cache = {}
exception_cache_lock = Lock()
warranty_cache = {}
warranty_cache_lock = Lock()
genealogy_cache = {}
genealogy_cache_lock = Lock()
warranty_token_cache = {"token": None, "fetched_at": None}
warranty_token_lock = Lock()
inventory_cache = {}
inventory_cache_lock = Lock()
device_details_cache = {}
device_details_cache_lock = Lock()
warranty_session = requests.Session()
pmx_client = PmxClient()


def extract_serial_number(record):
    for key in ("SN", "SERIAL_NR", "serialNumber", "serial_number"):
        value = record.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def extract_model_name(record):
    for key in ("MODEL", "MODEL_NAME", "model"):
        value = record.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def normalise_active_record(record):
    serial_number = extract_serial_number(record)
    if not serial_number:
        return None

    model = extract_model_name(record)
    status = str(record.get("STATUS") or record.get("status") or "").strip()
    service_request_id = str(record.get("serviceRequestId") or record.get("SR") or record.get("SERVICE_REQUEST_ID") or "").strip()
    service_request_status = str(record.get("serviceRequestStatus") or record.get("SR_STATUS") or "").strip()

    if not service_request_status:
        service_request_status = "Active"

    return {
        "serialNumber": serial_number,
        "model": model,
        "status": status,
        "serviceRequestId": service_request_id,
        "serviceRequestStatus": service_request_status,
    }


def coerce_over_tat_records(payload):
    if not isinstance(payload, list) or not payload:
        return []

    header, *rows = payload
    if not isinstance(header, list):
        return []

    records = []
    for row in rows:
        if not isinstance(row, list):
            continue
        entry = dict(zip(header, row))
        record = normalise_active_record(entry)
        if record:
            records.append(record)
    return records


def load_portal_session_headers():
    token = os.environ.get("PEGAP_AUTH_TOKEN")
    cookies = os.environ.get("PEGAP_COOKIES")
    if not token and not cookies:
        return None

    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if cookies:
        headers["Cookie"] = cookies
    return headers


def fetch_over_tat_records():
    response = pmx_client.get(
        OVER_TAT_ITEMS_URL,
        params=OVER_TAT_ITEMS_PARAMS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return coerce_over_tat_records(payload)


def normalise_yield_record(row):
    if not isinstance(row, list) or len(row) < 4:
        return None
    date, station, input_value, failed_value = row[0], row[1], row[2], row[3]
    try:
        input_count = int(input_value)
        failed_count = int(failed_value)
    except (TypeError, ValueError):
        return None
    pass_rate = round(((input_count - failed_count) / input_count) * 100, 2) if input_count else 0.0
    return {
        "date": str(date),
        "station": str(station),
        "input": input_count,
        "failed": failed_count,
        "passRate": pass_rate,
    }


def coerce_yield_records(payload):
    if not isinstance(payload, list):
        return []

    records = []
    for row in payload:
        record = normalise_yield_record(row)
        if record:
            records.append(record)
    return records


def fetch_status_transition_records(begin_date, end_date, transitions, timestamp_key):
    """Serials that transitioned FROM curr_status TO its paired status within the date
    range, per ERS_ITEM_TRACK history (same data ELM's Item Track tab shows).
    `transitions` is a list of (curr_status, status) pairs, OR'd together."""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not user or not password:
        return []

    transition_params = {}
    conditions = []
    for i, (curr_status, status) in enumerate(transitions):
        transition_params[f"curr_status{i}"] = curr_status
        transition_params[f"status{i}"] = status
        conditions.append(f"(T.CURR_STATUS = :curr_status{i} AND T.STATUS = :status{i})")

    query = """
        SELECT T.SERIAL_NR, T.CT_DATE, T.ITEM_NO, T.CURR_STATUS, T.CT_ID
        FROM ERS.ERS_ITEM_TRACK T
        JOIN ERS.ERS_RMA_INFO R ON R.ITEM_NO = T.ITEM_NO
        WHERE ({conditions})
          AND T.CT_DATE >= TO_DATE(:begin_date, 'YYYY-MM-DD')
          AND T.CT_DATE < TO_DATE(:end_date, 'YYYY-MM-DD') + 1
          AND R.PRODUCT_TYPE IN ({product_types})
        ORDER BY T.CT_DATE DESC
    """.format(
        conditions=" OR ".join(conditions),
        product_types=", ".join(f"'{code}'" for code in SURFACE_PRODUCT_TYPES),
    )
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, {
                **transition_params,
                "begin_date": begin_date,
                "end_date": end_date,
            })
            records = [
                {
                    "serialNumber": str(serial_nr).strip(),
                    timestamp_key: ct_date.isoformat(),
                    "itemNo": item_no,
                    "matchedStatus": matched_status,
                    "trackCtId": str(track_ct_id).strip() if track_ct_id else "",
                }
                for serial_nr, ct_date, item_no, matched_status, track_ct_id in cursor.fetchall()
                if serial_nr
            ]

    try:
        product_by_serial = fetch_product_map([record["serialNumber"] for record in records])
    except oracledb.Error:
        product_by_serial = {}
    for record in records:
        record["product"] = product_by_serial.get(record["serialNumber"], "")

    model_by_serial = {}
    for curr_status, _ in transitions:
        try:
            for station_record in fetch_station_serial_records(curr_status, begin_date, end_date):
                if station_record.get("model"):
                    model_by_serial[station_record["serialNumber"]] = station_record["model"]
        except (PmxAuthError, requests.RequestException):
            continue
    for record in records:
        record["model"] = model_by_serial.get(record["serialNumber"], "")

    rpl_reason_item_nos = [r["itemNo"] for r in records if r["matchedStatus"] == EXCHANGE_CURR_STATUS[0]]
    ffa_item_nos = [r["itemNo"] for r in records if r["matchedStatus"] == EXCHANGE_CURR_STATUS[1]]
    other_item_nos = [r["itemNo"] for r in records if r["matchedStatus"] not in EXCHANGE_CURR_STATUS]
    try:
        rpl_reason_memo_by_item_no = fetch_rpl_reason_memo_map(rpl_reason_item_nos)
    except oracledb.Error:
        rpl_reason_memo_by_item_no = {}
    try:
        ffa_memo_by_item_no = fetch_ffa_issue_memo_map(ffa_item_nos)
    except oracledb.Error:
        ffa_memo_by_item_no = {}

    # Disposition must come from the same station/event as this transition, not just
    # the most recent DISPOSITION_MATRIX_RESULT_CODE ever logged for the item (which
    # can be a stale value from an unrelated earlier repair cycle).
    try:
        rpl_disposition_by_item_no = fetch_latest_code_memo_map(rpl_reason_item_nos, DISPOSITION_FALLBACK_CODE_TYPE)
    except oracledb.Error:
        rpl_disposition_by_item_no = {}
    try:
        ffa_disposition_by_item_no = fetch_latest_code_memo_map(ffa_item_nos, DISPOSITION_CODE_TYPE)
    except oracledb.Error:
        ffa_disposition_by_item_no = {}
    try:
        other_disposition_by_item_no = fetch_disposition_map(other_item_nos)
    except oracledb.Error:
        other_disposition_by_item_no = {}

    ct_id_by_item_no = {}
    for record in records:
        item_no = record["itemNo"]
        matched_status = record["matchedStatus"]
        if matched_status == EXCHANGE_CURR_STATUS[0]:
            memo, code_value, code_loc, code_value2, ct_id = rpl_reason_memo_by_item_no.get(item_no, ("", "", "", "", ""))
            _, disposition_value, _, _, _ = rpl_disposition_by_item_no.get(item_no, ("", "", "", "", ""))
            # PAR-type dispositions often leave a generic Code Memo like "1";
            # Code Loc/Code Value2 (e.g. "MOTHERBOARD - 90-N41HM02A0") is the real issue detail then.
            if code_loc and code_value2:
                issue = f"{code_loc} - {code_value2}"
            elif memo and memo.strip().upper() not in RPL_REASON_NON_DESCRIPTIVE_MEMOS:
                issue = memo
            else:
                issue = code_value or memo
        elif matched_status == EXCHANGE_CURR_STATUS[1]:
            memo, code_value, _, _, ct_id = ffa_memo_by_item_no.get(item_no, ("", "", "", "", ""))
            _, disposition_value, _, _, _ = ffa_disposition_by_item_no.get(item_no, ("", "", "", "", ""))
            issue = memo or code_value
        elif matched_status == EXCHANGE_CURR_STATUS[2]:
            ct_id = record["trackCtId"]
            disposition_value = LOWLANE_CHECKOUT_DISPOSITION
            issue = LOWLANE_CHECKOUT_ISSUE
        else:
            ct_id = record["trackCtId"]
            disposition_value = other_disposition_by_item_no.get(item_no, "")
            issue = ""
        record["issue"] = issue
        record["disposition"] = disposition_value
        ct_id_by_item_no[item_no] = ct_id

    try:
        user_name_by_ct_id = fetch_user_name_map(ct_id_by_item_no.values())
    except oracledb.Error:
        user_name_by_ct_id = {}
    try:
        keyin_date_by_item_no = fetch_keyin_date_map([record["itemNo"] for record in records])
    except oracledb.Error:
        keyin_date_by_item_no = {}
    for record in records:
        item_no = record.pop("itemNo")
        record.pop("matchedStatus")
        record.pop("trackCtId")
        record["user"] = user_name_by_ct_id.get(ct_id_by_item_no.get(item_no), "")
        record["keyinDate"] = keyin_date_by_item_no.get(item_no, "")

    for record in records:
        record["serviceRequestId"] = ""
    try:
        token = get_warranty_token()
    except oracledb.Error:
        token = None
    if token:
        add_latest_service_requests(records, token)

    return records


def fetch_pack_records(begin_date, end_date):
    return fetch_status_transition_records(begin_date, end_date, [(PACK_CURR_STATUS, PACK_STATUS)], "packedAt")


def fetch_exchange_records(begin_date, end_date):
    transitions = [(cs, EXCHANGE_STATUS_BY_CURR_STATUS[cs]) for cs in EXCHANGE_CURR_STATUS]
    return fetch_status_transition_records(begin_date, end_date, transitions, "exchangedAt")


def fetch_exception_records(begin_date, end_date):
    return [
        record for record in fetch_exchange_records(begin_date, end_date)
        if record.get("disposition") == EXCEPTION_DISPOSITION
    ]


def get_cached_pack_records(cache_key):
    with pack_cache_lock:
        entry = pack_cache.get(cache_key)
    if not entry:
        return None
    records, cached_at = entry
    if datetime.now() - cached_at >= PACK_CACHE_TTL:
        return None
    return records


def set_cached_pack_records(cache_key, records):
    with pack_cache_lock:
        pack_cache[cache_key] = (records, datetime.now())


def get_cached_exchange_records(cache_key):
    with exchange_cache_lock:
        entry = exchange_cache.get(cache_key)
    if not entry:
        return None
    records, cached_at = entry
    if datetime.now() - cached_at >= EXCHANGE_CACHE_TTL:
        return None
    return records


def set_cached_exchange_records(cache_key, records):
    with exchange_cache_lock:
        exchange_cache[cache_key] = (records, datetime.now())


def get_cached_exception_records(cache_key):
    with exception_cache_lock:
        entry = exception_cache.get(cache_key)
    if not entry:
        return None
    records, cached_at = entry
    if datetime.now() - cached_at >= EXCEPTION_CACHE_TTL:
        return None
    return records


def set_cached_exception_records(cache_key, records):
    with exception_cache_lock:
        exception_cache[cache_key] = (records, datetime.now())


def fetch_station_yield_records(begin_date=None, end_date=None):
    if not begin_date or not end_date:
        end_date = datetime.now().date().isoformat()
        begin_date = (datetime.now().date() - timedelta(days=YIELD_STATIONS_LOOKBACK_DAYS)).isoformat()

    params = {
        "model": "Surface-SUR",
        "section": "PostTest",
        "begin": begin_date,
        "end": end_date,
        "buildId": "",
        "env": "PRO",
    }
    response = pmx_client.get(YIELD_STATIONS_URL, params=params, timeout=30)
    response.raise_for_status()
    return coerce_yield_records(response.json())


def fetch_station_serial_records(station, begin_date, end_date=None):
    params = {
        "model": "Surface-SUR",
        "station": station,
        "begin": begin_date,
        "end": end_date or begin_date,
    }
    response = pmx_client.get(STATION_SERIALS_URL, params=params, timeout=30)
    response.raise_for_status()

    workbook = openpyxl.load_workbook(BytesIO(response.content), data_only=True)
    worksheet = workbook.worksheets[0]
    rows = worksheet.iter_rows(values_only=True)
    header = [str(cell or "").strip().upper() for cell in next(rows, [])]

    def column_index(*names):
        for name in names:
            if name in header:
                return header.index(name)
        return None

    item_no_idx = column_index("ITEM_NO")
    model_idx = column_index("SN_MODEL")
    serial_idx = column_index("SERIAL_NR")
    station_idx = column_index("STATION")
    fail_idx = column_index("FAIL")
    code_idx = column_index("COMMITED_FAILURE_CODES")

    records = []
    for row in rows:
        serial_number = row[serial_idx] if serial_idx is not None else None
        if not serial_number:
            continue
        records.append({
            "serialNumber": str(serial_number).strip(),
            "model": str(row[model_idx]).strip() if model_idx is not None and row[model_idx] is not None else "",
            "itemNo": row[item_no_idx] if item_no_idx is not None else None,
            "station": str(row[station_idx]).strip() if station_idx is not None and row[station_idx] is not None else station,
            "failed": str(row[fail_idx]).strip().upper() == "Y" if fail_idx is not None else False,
            "failureCode": str(row[code_idx]).strip() if code_idx is not None and row[code_idx] is not None else "",
        })
    return records


DISPOSITION_CODE_TYPE = "DISPOSITION_MATRIX_RESULT_CODE"
DISPOSITION_FALLBACK_CODE_TYPE = "MSRSURRPL"


def fetch_disposition_map(item_nos):
    """"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    item_nos = sorted({item_no for item_no in item_nos if item_no is not None})
    if not item_nos or not user or not password:
        return {}

    placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
    params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
    params["code_type"] = DISPOSITION_CODE_TYPE
    params["fallback_code_type"] = DISPOSITION_FALLBACK_CODE_TYPE
    query = f"""
        SELECT ITEM_NO, CODE_TYPE, CODE_VALUE FROM (
            SELECT ITEM_NO, CODE_TYPE, CODE_VALUE,
                   ROW_NUMBER() OVER (
                       PARTITION BY ITEM_NO
                       ORDER BY CASE CODE_TYPE WHEN :code_type THEN 0 ELSE 1 END, CT_DATE DESC
                   ) AS RN
            FROM ERS.ERS_ITEM_CODE
            WHERE CODE_TYPE IN (:code_type, :fallback_code_type) AND ITEM_NO IN ({placeholders})
        )
        WHERE RN = 1
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (str(row[2]).strip() if row[2] is not None else "")
                for row in cursor.fetchall()
            }


RPL_REASON_CODE_TYPE = "RPL_REASON"
FFA_ISSUE_CODE_TYPE = "MSRSFFA"
# Generic/non-descriptive Code Memo values seen on RPL_REASON rows (e.g. "1", "ADP")
# that aren't a useful Issue description on their own, so Code Value is used instead.
RPL_REASON_NON_DESCRIPTIVE_MEMOS = {"1", "ADP"}


def fetch_rpl_reason_memo_map(item_nos):
  
    return fetch_latest_code_memo_map(item_nos, RPL_REASON_CODE_TYPE)


def fetch_ffa_issue_memo_map(item_nos):
    """"""
    return fetch_latest_code_memo_map(item_nos, FFA_ISSUE_CODE_TYPE)


def fetch_latest_code_memo_map(item_nos, code_type):
    """Returns {item_no: (code_memo, code_value, code_loc, code_value2, ct_id)} for the
    most recent ERS_ITEM_CODE row of the given CODE_TYPE per item."""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    item_nos = sorted({item_no for item_no in item_nos if item_no is not None})
    if not item_nos or not user or not password:
        return {}

    placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
    params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
    params["code_type"] = code_type
    query = f"""
        SELECT ITEM_NO, CODE_MEMO, CODE_VALUE, CODE_LOC, CODE_VALUE2, CT_ID FROM (
            SELECT ITEM_NO, CODE_MEMO, CODE_VALUE, CODE_LOC, CODE_VALUE2, CT_ID,
                   ROW_NUMBER() OVER (PARTITION BY ITEM_NO ORDER BY CT_DATE DESC, SEQ_NO DESC) AS RN
            FROM ERS.ERS_ITEM_CODE
            WHERE CODE_TYPE = :code_type AND ITEM_NO IN ({placeholders})
        )
        WHERE RN = 1
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (
                    str(row[1]).strip() if row[1] is not None else "",
                    str(row[2]).strip() if row[2] is not None else "",
                    str(row[3]).strip() if row[3] is not None else "",
                    str(row[4]).strip() if row[4] is not None else "",
                    str(row[5]).strip() if row[5] is not None else "",
                )
                for row in cursor.fetchall()
            }


def fetch_user_name_map(account_nos):
    """"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    account_nos = sorted({account_no for account_no in account_nos if account_no})
    if not account_nos or not user or not password:
        return {}

    placeholders = ",".join(f":acct{i}" for i in range(len(account_nos)))
    params = {f"acct{i}": account_no for i, account_no in enumerate(account_nos)}
    query = f"""
        SELECT ACCOUNT_NO, ACCOUNT_NAME FROM CC.ACCOUNT
        WHERE ACCOUNT_NO IN ({placeholders})
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }


KEYIN_CURR_STATUS = "KEYIN"


def fetch_keyin_date_map(item_nos):
    """Earliest ERS_ITEM_TRACK 'Track Date' where CURR_STATUS = 'KEYIN' per ITEM_NO,
    same as ELM's Item Track tab first row."""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    item_nos = sorted({item_no for item_no in item_nos if item_no is not None})
    if not item_nos or not user or not password:
        return {}

    placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
    params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
    params["curr_status"] = KEYIN_CURR_STATUS
    query = f"""
        SELECT ITEM_NO, CT_DATE FROM (
            SELECT ITEM_NO, CT_DATE,
                   ROW_NUMBER() OVER (PARTITION BY ITEM_NO ORDER BY CT_DATE ASC) AS RN
            FROM ERS.ERS_ITEM_TRACK
            WHERE CURR_STATUS = :curr_status AND ITEM_NO IN ({placeholders})
        )
        WHERE RN = 1
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (row[1].isoformat() if row[1] is not None else "")
                for row in cursor.fetchall()
            }


def fetch_idle_hours_map(serial_numbers):
    """Hours elapsed since each serial's most recent ERS_ITEM_TRACK entry, i.e. how
    long it has been sitting in its current station/status (ELM's Item Track Date)."""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    serial_numbers = sorted({sn for sn in serial_numbers if sn})
    if not serial_numbers or not user or not password:
        return {}

    placeholders = ",".join(f":sn{i}" for i in range(len(serial_numbers)))
    params = {f"sn{i}": sn for i, sn in enumerate(serial_numbers)}
    query = f"""
        SELECT SERIAL_NR, MAX(CT_DATE)
        FROM ERS.ERS_ITEM_TRACK
        WHERE SERIAL_NR IN ({placeholders})
        GROUP BY SERIAL_NR
    """
    now = datetime.now()
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: round((now - row[1]).total_seconds() / 3600, 2)
                for row in cursor.fetchall()
                if row[1] is not None
            }


def fetch_error_descriptions(station, item_nos):
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    item_nos = sorted({item_no for item_no in item_nos if item_no is not None})
    if not item_nos or not user or not password:
        return {}

    placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
    params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
    params["station"] = station
    query = f"""
        SELECT ITEM_NO, CODE_VALUE, DEF_DESCRIPTION FROM (
            SELECT proc.ITEM_NO AS ITEM_NO, code.CODE_VALUE AS CODE_VALUE,
                   def.DESCRIPTION AS DEF_DESCRIPTION,
                   ROW_NUMBER() OVER (PARTITION BY proc.ITEM_NO ORDER BY code.CT_DATE DESC) AS RN
            FROM ERS.ERS_PROCESS proc
            JOIN ERS.ERS_ITEM_CODE code
                ON code.ITEM_NO = proc.ITEM_NO
               AND code.RMA_SITE = proc.RMA_SITE
               AND code.REF_PROCESS_NO = proc.PROCESS_NO
            LEFT JOIN ERS.ERS_CF_DEF_CODE def
                ON def.CODE = code.CODE_VALUE
               AND def.CODE_GROUP = code.CODE_LOC
               AND def.RMA_SITE = code.RMA_SITE
            WHERE proc.STATION = :station AND proc.ITEM_NO IN ({placeholders})
        )
        WHERE RN = 1
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (
                    str(row[1]).strip() if row[1] is not None else "",
                    str(row[2]).strip() if row[2] is not None else "",
                )
                for row in cursor.fetchall()
            }


def fetch_products_by_item_no(item_nos):
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    item_nos = sorted({item_no for item_no in item_nos if item_no is not None})
    if not item_nos or not user or not password:
        return {}

    placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
    params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
    query = f"""
        SELECT ITEM_NO, MODEL
        FROM ERS.ERS_RMA_INFO
        WHERE ITEM_NO IN ({placeholders})
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }


def get_cached_station_serials(station, date, force_refresh):
    cache_key = (station, date)
    with station_serials_cache_lock:
        entry = station_serials_cache.get(cache_key)
        if not force_refresh and entry:
            records, cached_at = entry
            if datetime.now() - cached_at < STATION_SERIALS_CACHE_TTL:
                return records

    records = fetch_station_serial_records(station, date)
    try:
        product_by_serial = fetch_product_map([record["serialNumber"] for record in records])
    except oracledb.Error:
        product_by_serial = {}
    try:
        product_by_item_no = fetch_products_by_item_no([record["itemNo"] for record in records])
    except oracledb.Error:
        product_by_item_no = {}
    for record in records:
        record["product"] = product_by_serial.get(record["serialNumber"], "") or product_by_item_no.get(record["itemNo"], "")

    failed_item_nos = [record["itemNo"] for record in records if record["failed"]]
    try:
        descriptions = fetch_error_descriptions(station, failed_item_nos)
    except oracledb.Error:
        descriptions = {}
    for record in records:
        code_value, description = descriptions.get(record["itemNo"], ("", ""))
        record["errorCode"] = code_value or record["failureCode"]
        record["errorDescription"] = description
        del record["failureCode"]

    with station_serials_cache_lock:
        station_serials_cache[cache_key] = (records, datetime.now())
    return records


def load_fallback_serial_records():
    csv_path = Path.home() / "Downloads" / "DailyWIP_Rawdata.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"PMX Agile export not found: {csv_path}")

    records_by_serial = {}
    import csv
    with csv_path.open("r", encoding="utf-8-sig", newline="") as raw_data_file:
        for row in csv.DictReader(raw_data_file):
            serial_number = (row.get("SERIAL_NR") or row.get("SN") or "").strip()
            if serial_number:
                records_by_serial[serial_number] = {
                    "serialNumber": serial_number,
                    "model": (row.get("MODEL") or row.get("MODEL_NAME") or "").strip(),
                    "status": (row.get("STATUS") or "").strip(),
                }
    return list(records_by_serial.values())


def load_active_serial_records():
    try:
        records = fetch_over_tat_records()
        source = "pegap-tat-items" if records else None
    except (PmxAuthError, requests.RequestException):
        records = None
        source = None

    if not records:
        records, source = load_fallback_serial_records(), "fallback-csv"

    try:
        product_by_serial = fetch_product_map([r["serialNumber"] for r in records])
    except oracledb.Error:
        product_by_serial = {}

    for record in records:
        record["product"] = product_by_serial.get(record["serialNumber"], "")

    try:
        token = get_warranty_token()
        if token:
            records = add_latest_service_requests(records, token)
            records = add_genealogy_skus(records, token)
    except oracledb.Error:
        pass

    try:
        apply_functional_pn_mapping(records)
    except oracledb.Error:
        pass

    add_inventory_counts(records)

    try:
        idle_hours_by_serial = fetch_idle_hours_map([r["serialNumber"] for r in records])
    except oracledb.Error:
        idle_hours_by_serial = {}
    for record in records:
        record["idleHours"] = idle_hours_by_serial.get(record["serialNumber"])

    return records, source


def fetch_active_warranty_token():
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not user or not password:
        return None

    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT TOKEN FROM ers_edi_kore_token WHERE active_flag = 'Y'")
            token_row = cursor.fetchone()
    return token_row[0] if token_row else None


def get_warranty_token(force_refresh=False):
    with warranty_token_lock:
        fetched_at = warranty_token_cache["fetched_at"]
        if not force_refresh and fetched_at and datetime.now() - fetched_at < WARRANTY_TOKEN_CACHE_TTL:
            return warranty_token_cache["token"]

    token = fetch_active_warranty_token()
    with warranty_token_lock:
        warranty_token_cache["token"] = token
        warranty_token_cache["fetched_at"] = datetime.now()
    return token


def add_latest_service_requests(records, token):
    headers = {
        "Accept": "application/json",
        "SiteIdentifier": SITE_IDENTIFIER,
        "Authorization": f"Bearer {token}",
    }
    with ThreadPoolExecutor(max_workers=WARRANTY_MAX_WORKERS) as executor:
        return list(executor.map(lambda record: add_latest_service_request(record, headers), records))


def add_latest_service_request(record, headers):
    serial_number = record["serialNumber"]
    cached = get_cached_warranty_result(serial_number)
    if cached is not None:
        record["serviceRequestId"], record["serviceRequestStatus"] = cached
        return record

    record["serviceRequestId"] = ""
    record["serviceRequestStatus"] = "No claims"
    response = None
    max_attempts = 6
    for attempt in range(max_attempts):
        try:
            response = warranty_session.get(
                WARRANTY_URL.format(serial_number=serial_number),
                headers=headers,
                timeout=15,
            )
           
            if response.status_code not in (401, 403, 429, 500, 502, 503, 504):
                break
            time.sleep(0.7 * (attempt + 1) + random.uniform(0, 0.5))
        except requests.RequestException:
            if attempt == max_attempts - 1:
                record["serviceRequestStatus"] = "Warranty request failed"
                return record
            time.sleep(0.7 * (attempt + 1) + random.uniform(0, 0.5))

    try:
        if response is None:
            record["serviceRequestStatus"] = "Warranty request failed"
            return record
        response.raise_for_status()
        claims = response.json().get("claims", [])
        if claims:
            latest_claim = max(
                claims,
                key=lambda claim: datetime.fromisoformat(
                    claim.get("claimModifiedDate", "0001-01-01T00:00:00+00:00")
                ),
            )
            record["serviceRequestId"] = str(latest_claim.get("serviceRequestId") or "")
            record["serviceRequestStatus"] = "Found" if record["serviceRequestId"] else "Claim without SR"
    except requests.RequestException:
        record["serviceRequestStatus"] = f"Warranty HTTP {response.status_code}"
    except ValueError:
        record["serviceRequestStatus"] = "Invalid Warranty response"

    if record["serviceRequestStatus"] in ("Found", "No claims", "Claim without SR"):
        set_cached_warranty_result(serial_number, record["serviceRequestId"], record["serviceRequestStatus"])
    return record


def get_cached_warranty_result(serial_number):
    with warranty_cache_lock:
        entry = warranty_cache.get(serial_number)
    if not entry:
        return None
    service_request_id, service_request_status, cached_at = entry
    ttl = WARRANTY_CACHE_TTL if service_request_id else WARRANTY_NEGATIVE_CACHE_TTL
    if datetime.now() - cached_at >= ttl:
        return None
    return service_request_id, service_request_status


def extract_ship_unit_pn(payload):
    for assembly in payload.get("AssemblyCollection") or []:
        if str(assembly.get("AssemblyKey") or "").upper() == "SHIPUNIT":
            return str(assembly.get("PartNumber") or "").strip()
    return ""


def add_genealogy_skus(records, token):
    headers = {
        "Accept": "application/json",
        "SiteIdentifier": SITE_IDENTIFIER,
        "Authorization": f"Bearer {token}",
    }
    with ThreadPoolExecutor(max_workers=WARRANTY_MAX_WORKERS) as executor:
        return list(executor.map(lambda record: add_genealogy_sku(record, headers), records))


def add_genealogy_sku(record, headers):
    serial_number = record["serialNumber"]
    cached = get_cached_genealogy_sku(serial_number)
    if cached is not None:
        record["sku"] = cached
        return record

    record["sku"] = ""
    response = None
    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            response = warranty_session.get(
                GENEALOGY_URL.format(serial_number=serial_number),
                headers=headers,
                timeout=15,
            )
          
            if response.status_code not in (401, 403, 429, 500, 502, 503, 504):
                break
            time.sleep(0.5 * (attempt + 1) + random.uniform(0, 0.4))
        except requests.RequestException:
            if attempt == max_attempts - 1:
                return record
            time.sleep(0.5 * (attempt + 1) + random.uniform(0, 0.4))

    try:
        if response is None or response.status_code == 404:
            return record
        response.raise_for_status()
        record["sku"] = extract_ship_unit_pn(response.json())
    except (requests.RequestException, ValueError):
        return record

    set_cached_genealogy_sku(serial_number, record["sku"])
    return record


def get_cached_genealogy_sku(serial_number):
    with genealogy_cache_lock:
        entry = genealogy_cache.get(serial_number)
    if not entry:
        return None
    sku, cached_at = entry
    ttl = GENEALOGY_CACHE_TTL if sku else GENEALOGY_NEGATIVE_CACHE_TTL
    if datetime.now() - cached_at >= ttl:
        return None
    return sku


def set_cached_genealogy_sku(serial_number, sku):
    with genealogy_cache_lock:
        genealogy_cache[serial_number] = (sku, datetime.now())


def fetch_functional_pn_map(ship_unit_pns):
    """"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    ship_unit_pns = sorted({pn for pn in ship_unit_pns if pn})
    if not ship_unit_pns or not user or not password:
        return {}

    placeholders = ",".join(f":pn{i}" for i in range(len(ship_unit_pns)))
    params = {f"pn{i}": pn for i, pn in enumerate(ship_unit_pns)}
    query = f"""
        SELECT SHIP_UNIT_PN, FUNCTIONAL_PN
        FROM ERS.ERS_CF_KORE_GPL_MPG
        WHERE FUNCTIONAL_PN_NO = 1 AND SHIP_UNIT_PN IN ({placeholders})
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }


def apply_functional_pn_mapping(records):
    functional_pn_by_ship_unit = fetch_functional_pn_map([record.get("sku") for record in records])
    for record in records:
        ship_unit_pn = record.get("sku")
        if ship_unit_pn:
            record["sku"] = functional_pn_by_ship_unit.get(ship_unit_pn) or ship_unit_pn


def fetch_internal_part_no_map(cust_pns):
    """"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    cust_pns = sorted({pn for pn in cust_pns if pn})
    if not cust_pns or not user or not password:
        return {}

    placeholders = ",".join(f":pn{i}" for i in range(len(cust_pns)))
    params = {f"pn{i}": pn for i, pn in enumerate(cust_pns)}
    query = f"""
        SELECT CUST_PN, PART_NO
        FROM CC.WM_PART_CUSTOMER
        WHERE CUST_CODE = 'KORE_PMX' AND CUST_PN IN ({placeholders})
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }


def fetch_inventory_details(part_nos):
    """'"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    part_nos = sorted({pn for pn in part_nos if pn})
    if not part_nos or not user or not password:
        return {}

    placeholders = ",".join(f":pn{i}" for i in range(len(part_nos)))
    params = {f"pn{i}": pn for i, pn in enumerate(part_nos)}
    params["warehouse_like"] = INVENTORY_WAREHOUSE_LIKE
    query = f"""
        SELECT PART_NO, LOC_ID
        FROM CC.WM_SLC_SERIAL
        WHERE PART_NO IN ({placeholders})
          AND STATUS = 'Y'
          AND LOC_ID LIKE :warehouse_like
    """
    details = {}
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            for part_no, loc_id in cursor.fetchall():
                entry = details.setdefault(part_no, {"count": 0, "locations": set()})
                entry["count"] += 1
                match = LOCATION_CODE_PATTERN.search(loc_id or "")
                if match:
                    entry["locations"].add(match.group(0))
    return {
        part_no: {"count": entry["count"], "locations": sorted(entry["locations"])}
        for part_no, entry in details.items()
    }


def parse_loc_id(loc_id):
    """"""
    loc_id = loc_id or ""
    match = LOCATION_CODE_PATTERN.search(loc_id)
    if not match:
        return {"warehouse": "", "location": "", "lot": ""}
    prefix = loc_id[: match.start()]
    warehouse = prefix[4:] if prefix.startswith("LOCL") else prefix
    lot = loc_id[match.end():].lstrip("-")
    return {"warehouse": warehouse, "location": match.group(0), "lot": lot}


def fetch_inventory_profile(part_no):
    """"""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not part_no or not user or not password:
        return []

    query = """
        SELECT LOC_ID
        FROM CC.WM_SLC_SERIAL
        WHERE PART_NO = :part_no
          AND STATUS = 'Y'
          AND LOC_ID LIKE :warehouse_like
    """
    groups = {}
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, {"part_no": part_no, "warehouse_like": INVENTORY_WAREHOUSE_LIKE})
            for (loc_id,) in cursor.fetchall():
                parsed = parse_loc_id(loc_id)
                key = (parsed["warehouse"], parsed["location"], parsed["lot"])
                groups[key] = groups.get(key, 0) + 1

    return [
        {
            "warehouse": warehouse,
            "location": location,
            "lot": lot,
            "allocatedQty": 0,
            "availableQty": count,
        }
        for (warehouse, location, lot), count in sorted(groups.items())
    ]


def get_cached_inventory_details(sku):
    with inventory_cache_lock:
        entry = inventory_cache.get(sku)
    if not entry:
        return None
    details, cached_at = entry
    if datetime.now() - cached_at >= INVENTORY_CACHE_TTL:
        return None
    return details


def set_cached_inventory_details(sku, details):
    with inventory_cache_lock:
        inventory_cache[sku] = (details, datetime.now())


def add_inventory_counts(records):
    skus = sorted({record.get("sku") for record in records if record.get("sku")})
    if not skus:
        return

    uncached_skus = [sku for sku in skus if get_cached_inventory_details(sku) is None]
    if uncached_skus:
        try:
            part_no_by_sku = fetch_internal_part_no_map(uncached_skus)
            details_by_part_no = fetch_inventory_details(part_no_by_sku.values())
            for sku in uncached_skus:
                part_no = part_no_by_sku.get(sku)
                set_cached_inventory_details(sku, details_by_part_no.get(part_no, {"count": 0, "locations": []}))
        except oracledb.Error:
            pass

    for record in records:
        details = get_cached_inventory_details(record.get("sku"))
        record["inv"] = details["count"] if details else ""
        record["loc"] = ", ".join(details["locations"]) if details else ""


def first_present(source, keys, default=""):
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return default


COUNTRY_NAMES = {
    "US": "United States", "CA": "Canada", "CN": "China", "VN": "Vietnam",
    "GB": "United Kingdom", "DE": "Germany", "FR": "France", "AU": "Australia",
    "JP": "Japan", "MX": "Mexico", "BR": "Brazil", "IN": "India",
}


def normalise_warranty_plan(plan):
    in_warranty = plan.get("inWarranty")
    if in_warranty is None:
        end_date = plan.get("endDate")
        try:
            in_warranty = bool(end_date) and datetime.fromisoformat(end_date) > datetime.now(timezone.utc)
        except ValueError:
            in_warranty = False
    return {
        "planName": first_present(plan, ("serviceContractType", "planName", "plan_name", "name")),
        "planCode": first_present(plan, ("serviceContractTypeCode", "planCode", "plan_code", "code")),
        "startDate": first_present(plan, ("startDate", "start_date")),
        "endDate": first_present(plan, ("endDate", "end_date")),
        "inWarranty": bool(in_warranty),
    }


def normalise_claim(claim):
    problem_code = first_present(claim, ("problemCode", "issueCode", "issue_code"))
    utilized_code = first_present(claim, ("utilizedProblemCode",))
    issue_code = f"{problem_code} / {utilized_code}" if problem_code and utilized_code else (problem_code or utilized_code)
    return {
        "orderId": first_present(claim, ("claimOrderId", "orderId", "order_id")),
        "serviceRequestId": str(first_present(claim, ("serviceRequestId", "service_request_id"), "")),
        "status": first_present(claim, ("claimStatus", "status")),
        "type": first_present(claim, ("claimType", "type", "repairType")),
        "issueCode": issue_code,
        "deviceReturned": claim.get("isDeviceReturned", claim.get("deviceReturned")),
        "entitlement": first_present(claim, ("entitlementCode", "entitlement")),
        "warrantyId": first_present(claim, ("warrantyId", "warranty_id")),
        "createdDate": first_present(claim, ("claimCreatedDate", "createdDate", "created_date")),
        "modifiedDate": first_present(claim, ("claimModifiedDate", "modifiedDate", "modified_date")),
    }


def normalise_component_part(part):
    return {
        "partNumber": first_present(part, ("partNumber", "part_number")),
        "partType": first_present(part, ("partType", "part_type")),
        "location": first_present(part, ("location",)),
        "manufacturedDatetime": first_present(part, ("manufacturedDatetime", "manufactured_datetime")),
        "countryOfOrigin": first_present(part, ("countryOfOrigin", "country_of_origin")),
    }


def fetch_device_details(serial_number, token):
    headers = {
        "Accept": "application/json",
        "SiteIdentifier": SITE_IDENTIFIER,
        "Authorization": f"Bearer {token}",
    }
    response = None
    max_attempts = 3
    for attempt in range(max_attempts):
        response = warranty_session.get(
            WARRANTY_URL.format(serial_number=serial_number),
            headers=headers,
            timeout=15,
        )
     
        if response.status_code not in (401, 403, 429, 500, 502, 503, 504):
            break
        if attempt < max_attempts - 1:
            time.sleep(0.5 * (attempt + 1) + random.uniform(0, 0.4))
    response.raise_for_status()
    payload = response.json()

    part = payload.get("part") or {}
    repair_product = part.get("repairProduct") or {}
    registration = payload.get("registration") or {}
    attributes = payload.get("attributes") or {}
    component_parts = payload.get("componentParts") or []

    warranties = [normalise_warranty_plan(plan) for plan in (payload.get("warranties") or [])]
    claims = [normalise_claim(claim) for claim in (payload.get("claims") or [])]
    claims.sort(key=lambda claim: claim["modifiedDate"] or "", reverse=True)
    parts = [normalise_component_part(item) for item in component_parts]

    operating_system = ""
    for item in reversed(component_parts):
        operating_system = item.get("osVerShort") or item.get("osVerLong") or ""
        if operating_system:
            break

    country_code = str(first_present(registration, ("countryCode",), "")).strip().upper()

    return {
        "serialNumber": first_present(payload, ("serialNumber", "serial_number"), serial_number),
        "partNumber": first_present(part, ("partNumber", "part_number")),
        "sku": first_present(part, ("sku", "repairSku")) or first_present(repair_product, ("sku",)),
        "series": first_present(repair_product, ("series",)) or first_present(part, ("msPressSeriesName",)),
        "imageUri": first_present(repair_product, ("imageUri", "image_uri")),
        "countryOfOrigin": first_present(attributes, ("countryOfOrigin",)),
        "operatingSystem": operating_system,
        "registrationStatus": first_present(payload, ("status", "registrationStatus")),
        "registrationDate": first_present(registration, ("registrationDatetime", "registrationDate")),
        "profile": first_present(registration, ("registrationProfile", "profile")),
        "marketplace": COUNTRY_NAMES.get(country_code, country_code) or first_present(part, ("marketplaceName",)),
        "passportId": str(first_present(registration, ("passportId",), "") or ""),
        "customerId": str(first_present(registration, ("customerId",), "") or ""),
        "warranties": warranties,
        "claims": claims,
        "componentParts": parts,
        "raw": payload,
    }


def get_cached_device_details(serial_number):
    with device_details_cache_lock:
        entry = device_details_cache.get(serial_number)
    if not entry:
        return None
    details, cached_at = entry
    if datetime.now() - cached_at >= DEVICE_DETAILS_CACHE_TTL:
        return None
    return details


def set_cached_device_details(serial_number, details):
    with device_details_cache_lock:
        device_details_cache[serial_number] = (details, datetime.now())


def set_cached_warranty_result(serial_number, service_request_id, service_request_status):
    with warranty_cache_lock:
        warranty_cache[serial_number] = (service_request_id, service_request_status, datetime.now())


SURFACE_COLOR_KEYWORDS = (
    "MATTE BLACK", "COBALT BLUE", "MINERAL BLUE", "POPPY RED", "ICE BLUE",
    "PLATINUM", "GRAPHITE", "SANDSTONE", "BURGUNDY", "STARLIGHT", "SILVER",
    "FOREST", "BLACK", "DUNE", "SAGE", "MOSS", "HYDRANGEA", "OCEAN", "SAPPHIRE", "EMERALD", "VIOLET",
)


def extract_color_from_part_desc(part_desc):
    if not part_desc:
        return ""
    tokens = [token.strip() for token in part_desc.split(",") if token.strip()]
    if not tokens:
        return ""
    upper_tokens = {token.upper() for token in tokens}

    for keyword in SURFACE_COLOR_KEYWORDS:
        if keyword in upper_tokens:
            return keyword.title()
    return ""


def fetch_rma_info_value_map(serial_numbers, column):
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not serial_numbers or not user or not password:
        return {}

    placeholders = ",".join(f":sn{i}" for i in range(len(serial_numbers)))
    params = {f"sn{i}": serial for i, serial in enumerate(serial_numbers)}

    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        item_by_serial = {}
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT SERIAL_NR, ITEM_NO FROM (
                    SELECT SERIAL_NR, ITEM_NO,
                           ROW_NUMBER() OVER (PARTITION BY SERIAL_NR ORDER BY CT_DATE DESC) AS RN
                    FROM ERS.ERS_ITEM_TRACK
                    WHERE SERIAL_NR IN ({placeholders})
                )
                WHERE RN = 1
                """,
                params,
            )
            for serial_nr, item_no in cursor.fetchall():
                if serial_nr is not None and item_no is not None:
                    item_by_serial[str(serial_nr).strip()] = item_no

        item_nos = sorted(set(item_by_serial.values()))
        if not item_nos:
            return {}

        item_placeholders = ",".join(f":item{i}" for i in range(len(item_nos)))
        item_params = {f"item{i}": item_no for i, item_no in enumerate(item_nos)}
        with connection.cursor() as cursor:
            
            cursor.execute(
                f"SELECT ITEM_NO, {column} FROM ERS.ERS_RMA_INFO WHERE ITEM_NO IN ({item_placeholders})",
                item_params,
            )
            value_by_item = {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }

    return {
        serial: value_by_item.get(item_no, "")
        for serial, item_no in item_by_serial.items()
    }


def fetch_product_map(serial_numbers):
    return fetch_rma_info_value_map(serial_numbers, "MODEL")


def fetch_color_map(serial_numbers):
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not serial_numbers or not user or not password:
        return {}

    placeholders = ",".join(f":sn{i}" for i in range(len(serial_numbers)))
    params = {f"sn{i}": serial for i, serial in enumerate(serial_numbers)}
    query = f"""
        SELECT SERIAL_NR, PART_DESC FROM (
            SELECT SERIAL_NR, PART_DESC,
                   ROW_NUMBER() OVER (PARTITION BY SERIAL_NR ORDER BY CT_DATE DESC) AS RN
            FROM ERS.ERS_KORE_CROSS_DOCK_PACK_DETAIL
            WHERE SERIAL_NR IN ({placeholders})
        )
        WHERE RN = 1
    """
    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            part_desc_by_serial = {
                row[0]: (str(row[1]).strip() if row[1] is not None else "")
                for row in cursor.fetchall()
            }

    return {
        serial: extract_color_from_part_desc(part_desc)
        for serial, part_desc in part_desc_by_serial.items()
    }


def retry_on_error(func, *args, attempts=2, delay=0.6, exceptions=(Exception,), **kwargs):
    last_error = None
    for attempt in range(attempts):
        try:
            return func(*args, **kwargs)
        except exceptions as error:
            last_error = error
            if attempt < attempts - 1:
                time.sleep(delay)
    raise last_error


def fetch_product_and_color_for_serial(serial_number):
    """Single-connection lookup for one serial, used by the device-details endpoint
    so a flaky Oracle round trip doesn't need two separate connections to fail."""
    user = os.environ.get("NPI_DB_USER")
    password = os.environ.get("NPI_DB_PASSWORD")
    if not serial_number or not user or not password:
        return "", ""

    with oracledb.connect(user=user, password=password, dsn=ORACLE_DSN) as connection:
        model = ""
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT R.MODEL FROM (
                    SELECT ITEM_NO, ROW_NUMBER() OVER (ORDER BY CT_DATE DESC) AS RN
                    FROM ERS.ERS_ITEM_TRACK WHERE SERIAL_NR = :sn
                ) T
                JOIN ERS.ERS_RMA_INFO R ON R.ITEM_NO = T.ITEM_NO
                WHERE T.RN = 1
                """,
                {"sn": serial_number},
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                model = str(row[0]).strip()

        color = ""
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT PART_DESC FROM (
                    SELECT PART_DESC, ROW_NUMBER() OVER (ORDER BY CT_DATE DESC) AS RN
                    FROM ERS.ERS_KORE_CROSS_DOCK_PACK_DETAIL WHERE SERIAL_NR = :sn
                )
                WHERE RN = 1
                """,
                {"sn": serial_number},
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                color = extract_color_from_part_desc(str(row[0]))

    return model, color


class PerformanceRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        request_url = urlparse(self.path)
        if request_url.path == "/api/device-details":
            return self.get_device_details(request_url)
        if request_url.path == "/api/station-yield":
            return self.get_station_yield(request_url)
        if request_url.path == "/api/station-serials":
            return self.get_station_serials(request_url)
        if request_url.path == "/api/pack-serials":
            return self.get_pack_serials(request_url)
        if request_url.path == "/api/exchange-serials":
            return self.get_exchange_serials(request_url)
        if request_url.path == "/api/exception-serials":
            return self.get_exception_serials(request_url)
        if request_url.path != "/api/active-sns":
            return super().do_GET()
        try:
            force_refresh = parse_qs(request_url.query).get("refresh") == ["1"]
            serial_numbers, updated_at, source = self.get_active_serial_numbers(force_refresh)
            self.send_json(200, {
                "count": len(serial_numbers),
                "serialNumbers": serial_numbers,
                "updatedAt": updated_at.isoformat(),
                "source": source,
            })
        except (KeyError, requests.RequestException, FileNotFoundError, oracledb.Error) as error:
            self.send_json(500, {"error": f"Unable to load active serial numbers: {error}"})

    def get_station_yield(self, request_url):
        try:
            query = parse_qs(request_url.query)
            force_refresh = query.get("refresh") == ["1"]
            begin_date = (query.get("begin") or [""])[0].strip() or None
            end_date = (query.get("end") or [""])[0].strip() or None
            records, updated_at = self.get_cached_station_yield(begin_date, end_date, force_refresh)
            self.send_json(200, {
                "count": len(records),
                "records": records,
                "updatedAt": updated_at.isoformat(),
            })
        except (PmxAuthError, requests.RequestException) as error:
            self.send_json(502, {"error": f"Unable to load station yield: {error}"})

    def get_cached_station_yield(self, begin_date, end_date, force_refresh):
        cache_key = (begin_date, end_date)
        with yield_cache_lock:
            entry = yield_cache.get(cache_key)
            if not force_refresh and entry:
                records, updated_at = entry
                if datetime.now() - updated_at < YIELD_CACHE_TTL:
                    return records, updated_at

        records = fetch_station_yield_records(begin_date, end_date)
        updated_at = datetime.now()
        with yield_cache_lock:
            yield_cache[cache_key] = (records, updated_at)
        return records, updated_at

    def get_station_serials(self, request_url):
        query = parse_qs(request_url.query)
        station = (query.get("station") or [""])[0].strip()
        date = (query.get("date") or [""])[0].strip()
        if not station or not date:
            return self.send_json(400, {"error": "Missing station or date parameter."})

        force_refresh = query.get("refresh") == ["1"]
        try:
            records = get_cached_station_serials(station, date, force_refresh)
            self.send_json(200, {
                "count": len(records),
                "failedCount": sum(1 for record in records if record["failed"]),
                "records": records,
            })
        except (PmxAuthError, requests.RequestException) as error:
            self.send_json(502, {"error": f"Unable to load station serials: {error}"})
        except oracledb.Error as error:
            self.send_json(500, {"error": f"Unable to load station serials: {error}"})

    def get_pack_serials(self, request_url):
        query = parse_qs(request_url.query)
        today = datetime.now().date().isoformat()
        begin_date = (query.get("begin") or [""])[0].strip() or (query.get("date") or [""])[0].strip() or today
        end_date = (query.get("end") or [""])[0].strip() or begin_date
        force_refresh = query.get("refresh") == ["1"]
        cache_key = (begin_date, end_date)
        try:
            records = None if force_refresh else get_cached_pack_records(cache_key)
            if records is None:
                records = fetch_pack_records(begin_date, end_date)
                set_cached_pack_records(cache_key, records)
            self.send_json(200, {
                "beginDate": begin_date,
                "endDate": end_date,
                "count": len(records),
                "records": records,
            })
        except oracledb.Error as error:
            self.send_json(500, {"error": f"Unable to load pack records: {error}"})

    def get_exchange_serials(self, request_url):
        query = parse_qs(request_url.query)
        today = datetime.now().date().isoformat()
        begin_date = (query.get("begin") or [""])[0].strip() or (query.get("date") or [""])[0].strip() or today
        end_date = (query.get("end") or [""])[0].strip() or begin_date
        force_refresh = query.get("refresh") == ["1"]
        cache_key = (begin_date, end_date)
        try:
            records = None if force_refresh else get_cached_exchange_records(cache_key)
            if records is None:
                records = fetch_exchange_records(begin_date, end_date)
                set_cached_exchange_records(cache_key, records)
            self.send_json(200, {
                "beginDate": begin_date,
                "endDate": end_date,
                "count": len(records),
                "records": records,
            })
        except oracledb.Error as error:
            self.send_json(500, {"error": f"Unable to load exchange records: {error}"})

    def get_exception_serials(self, request_url):
        query = parse_qs(request_url.query)
        today = datetime.now().date().isoformat()
        begin_date = (query.get("begin") or [""])[0].strip() or (query.get("date") or [""])[0].strip() or today
        end_date = (query.get("end") or [""])[0].strip() or begin_date
        force_refresh = query.get("refresh") == ["1"]
        cache_key = (begin_date, end_date)
        try:
            records = None if force_refresh else get_cached_exception_records(cache_key)
            if records is None:
                records = fetch_exception_records(begin_date, end_date)
                set_cached_exception_records(cache_key, records)
            self.send_json(200, {
                "beginDate": begin_date,
                "endDate": end_date,
                "count": len(records),
                "records": records,
            })
        except oracledb.Error as error:
            self.send_json(500, {"error": f"Unable to load exception records: {error}"})

    def get_device_details(self, request_url):
        query = parse_qs(request_url.query)
        serial_number = (query.get("sn") or [""])[0].strip()
        if not serial_number:
            return self.send_json(400, {"error": "Missing sn parameter."})

        force_refresh = query.get("refresh") == ["1"]
        try:
            details = None if force_refresh else get_cached_device_details(serial_number)
            if details is None:
                token = get_warranty_token(force_refresh=force_refresh)
                if not token:
                    return self.send_json(503, {"error": "Warranty token unavailable."})
                details = retry_on_error(
                    fetch_device_details, serial_number, token,
                    exceptions=(requests.ConnectionError, requests.Timeout),
                )
                try:
                    details["product"], details["color"] = retry_on_error(
                        fetch_product_and_color_for_serial, serial_number,
                        exceptions=(oracledb.Error,),
                    )
                except oracledb.Error:
                    details["product"] = ""
                    details["color"] = ""
                try:
                    genealogy_record = {"serialNumber": serial_number}
                    genealogy_headers = {
                        "Accept": "application/json",
                        "SiteIdentifier": SITE_IDENTIFIER,
                        "Authorization": f"Bearer {token}",
                    }
                    add_genealogy_sku(genealogy_record, genealogy_headers)
                    apply_functional_pn_mapping([genealogy_record])
                    part_no = fetch_internal_part_no_map([genealogy_record["sku"]]).get(genealogy_record["sku"])
                    details["inventoryProfile"] = fetch_inventory_profile(part_no) if part_no else []
                except oracledb.Error:
                    details["inventoryProfile"] = []
                set_cached_device_details(serial_number, details)
            self.send_json(200, details)
        except requests.HTTPError as error:
            status_code = error.response.status_code if error.response is not None else 502
            self.send_json(status_code, {"error": f"Warranty API error: {error}"})
        except requests.RequestException as error:
            self.send_json(502, {"error": f"Unable to load device details: {error}"})
        except oracledb.Error as error:
            self.send_json(500, {"error": f"Unable to load device details: {error}"})

    def get_active_serial_numbers(self, force_refresh):
        with cache_lock:
            updated_at = active_serials_cache["updated_at"]
            cache_is_fresh = updated_at and datetime.now() - updated_at < CACHE_TTL
            if force_refresh or not cache_is_fresh:
                active_serials_cache["records"], active_serials_cache["source"] = self.fetch_active_serial_numbers()
                active_serials_cache["updated_at"] = datetime.now()
            return active_serials_cache["records"], active_serials_cache["updated_at"], active_serials_cache.get("source", "unknown")

    def fetch_active_serial_numbers(self):
        serial_numbers, source = load_active_serial_records()
        return serial_numbers, source

    def send_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            return

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, must-revalidate")
        super().end_headers()


if __name__ == "__main__":
    os.chdir(APP_DIR)
    # 0.0.0.0 exposes the dashboard to the corporate LAN; queries/credentials stay server-side.
    host = os.environ.get("PERFORMANCE_HOST", "0.0.0.0")
    port = int(os.environ.get("PERFORMANCE_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), PerformanceRequestHandler)
    print(f"Performance is available at http://{host}:{port}")
    server.serve_forever()