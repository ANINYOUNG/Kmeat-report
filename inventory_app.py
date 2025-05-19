# inventory_app.py (Google Drive API - .xlsx 직접 로딩 버전 - 요일 표시 및 용어 변경 + 디버깅 코드 추가)

import streamlit as st

# --- 1. 페이지 설정 (가장 먼저 실행되어야 하는 Streamlit 명령) ---
st.set_page_config(page_title="데이터 분석 대시보드", layout="wide", initial_sidebar_state="expanded")

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os # 환경 변수 사용을 위해 추가
import traceback
import plotly.express as px
import json # JSON 처리를 위해 추가
import io # 바이트 스트림 처리를 위해 추가

# Google Drive API 관련 라이브러리
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# --- 한국어 요일 리스트 ---
KOREAN_DAYS = ['월', '화', '수', '목', '금', '토', '일']

# --- Google API 인증 및 Drive 서비스 클라이언트 생성 ---
drive_service = None
SERVICE_ACCOUNT_LOADED = False


# --- !!! 중요: 디버깅 코드 시작 !!! ---
# Streamlit Cloud 환경인지, Secrets가 제대로 로드되는지 확인하기 위한 코드입니다.
# 문제가 해결된 후에는 이 디버깅 관련 st.write, st.error, st.text 문구들을 삭제하거나 주석 처리해주세요.

st.write(f"환경 변수 IS_STREAMLIT_CLOUD 값: {os.getenv('IS_STREAMLIT_CLOUD')}") # Streamlit Cloud에서 설정하는 환경 변수 값 확인
IS_CLOUD_ENVIRONMENT = os.getenv('IS_STREAMLIT_CLOUD') == 'true'
st.write(f"IS_CLOUD_ENVIRONMENT 변수 평가 결과: {IS_CLOUD_ENVIRONMENT}") # 위 환경 변수를 기반으로 클라우드 환경인지 여부

if IS_CLOUD_ENVIRONMENT:
    st.write("클라우드 환경으로 판단됨. st.secrets에서 인증 정보 로드를 시도합니다...")
    if "google_creds_json" not in st.secrets:
        st.error("오류: st.secrets에 'google_creds_json' 키가 없습니다! Streamlit Cloud 대시보드에서 Secrets 설정을 확인하세요.")
        # st.write("현재 st.secrets에 있는 키 목록 (민감한 값은 제외):", st.secrets.to_dict().keys()) # 어떤 키들이 있는지 확인용
    else:
        try:
            creds_json_str = st.secrets["google_creds_json"]
            st.success("'google_creds_json' 키를 st.secrets에서 성공적으로 가져왔습니다.")
            # st.text("가져온 JSON 문자열의 일부 (앞 100자): " + creds_json_str[:100] + "...") # 실제 값의 일부를 보고 싶을 때 (주의!)

            creds_dict = json.loads(creds_json_str) # JSON 문자열을 파이썬 딕셔너리로 변환
            st.success("st.secrets에서 가져온 JSON 문자열을 성공적으로 파싱했습니다.")

            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            drive_service = build('drive', 'v3', credentials=creds)
            SERVICE_ACCOUNT_LOADED = True
            st.success("Google Drive 서비스가 성공적으로 초기화되었습니다. SERVICE_ACCOUNT_LOADED = True")
        except json.JSONDecodeError as e_json:
            st.error(f"JSON 파싱 오류: st.secrets의 'google_creds_json' 내용을 파싱하는 중 오류가 발생했습니다: {e_json}")
            st.error("Secrets에 입력된 JSON 문자열이 올바른 형식인지, 특히 줄바꿈, 따옴표 등이 정확한지 확인해주세요.")
            st.text("문제가 되는 JSON 문자열의 일부 (앞 200자): " + creds_json_str[:200] + "...") # 문제 부분 확인용
        except KeyError: # 이 경우는 "google_creds_json" not in st.secrets 에서 이미 걸러지지만, 만약을 위해 남겨둡니다.
            st.error("KeyError: st.secrets에서 'google_creds_json' 키를 찾을 수 없습니다.")
            if 'sidebar_error_displayed' not in st.session_state: # 기존 오류 메시지 유지
                st.sidebar.error("오류: 클라우드 Secrets에 'google_creds_json' 키가 없습니다. 관리자에게 문의하세요.")
                st.session_state.sidebar_error_displayed = True
        except Exception as e_secrets:
            st.error(f"클라우드 Secrets 처리 중 예상치 못한 예외 발생: {e_secrets}")
            if 'sidebar_error_displayed' not in st.session_state: # 기존 오류 메시지 유지
                st.sidebar.error(f"오류: 클라우드 Secrets 인증 중 예외 발생: {e_secrets}")
                st.session_state.sidebar_error_displayed = True
else:
    st.write("클라우드 환경이 아님 (또는 IS_CLOUD_ENVIRONMENT가 False). 로컬 파일에서 인증 정보 로드를 시도합니다.")
# --- !!! 중요: 디버깅 코드 끝 !!! ---


# 로컬 환경일 경우 (SERVICE_ACCOUNT_LOADED가 False일 때만 실행)
if not SERVICE_ACCOUNT_LOADED:
    SERVICE_ACCOUNT_FILE = r"C:\Users\kmeat 1f\Documents\googleaiy\clear-shadow-444503-q4-88934382a4ce.json" # 사용자 실제 경로
    st.write(f"로컬 서비스 계정 키 파일 경로: {SERVICE_ACCOUNT_FILE}") # 로컬 파일 경로 확인용 (클라우드에서는 이 부분이 실행되면 안됨)
    try:
        if os.path.exists(SERVICE_ACCOUNT_FILE):
            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
            drive_service = build('drive', 'v3', credentials=creds)
            SERVICE_ACCOUNT_LOADED = True
            st.info(f"로컬 파일({SERVICE_ACCOUNT_FILE})에서 성공적으로 인증 정보를 로드했습니다. SERVICE_ACCOUNT_LOADED = True")
        else:
            if not IS_CLOUD_ENVIRONMENT: # 클라우드 환경이 아닐 때만 이 경고를 표시 (디버깅 메시지와 중복될 수 있음)
                st.warning(f"경고: 서비스 계정 키 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}. Google Drive 연동 기능이 제한될 수 있습니다.")
                if 'sidebar_error_displayed' not in st.session_state:
                    st.session_state.sidebar_error_displayed = True
    except Exception as e_local:
        if 'sidebar_error_displayed' not in st.session_state:
            st.sidebar.warning(f"경고: 로컬 키 파일 인증 중 오류 발생 ({SERVICE_ACCOUNT_FILE}): {e_local}. Google Drive 연동 기능이 제한될 수 있습니다.")
            st.session_state.sidebar_error_displayed = True


if not SERVICE_ACCOUNT_LOADED or drive_service is None:
    st.error("### 중요: Google Drive API 인증 실패! ###")
    st.error("Google Drive 서비스가 초기화되지 않았습니다. 앱 상단의 디버깅 메시지나 Streamlit Cloud 로그를 확인하여 원인을 파악하세요.")
    if 'critical_auth_error_displayed' not in st.session_state:
        st.session_state.critical_auth_error_displayed = True
    st.stop() # 인증 안되면 앱 중단

# --- Google Drive 파일 ID 정의 ---
SM_FILE_ID = "1tRljdvOpp4fITaVEXvoL9mNveNg2qt4p"
PURCHASE_FILE_ID = "1AgKl29yQ80sTDszLql6oBnd9FnLWf8oR" # 입고 파일 ID
SALES_FILE_ID = "1h-V7kIoInXgGLll7YBW5V_uZdF3Q1PdY"   # 출고 파일 ID
ERP_FILE_ID = "1Lbtwenw8LcDaj94_J4kKTjoWQY7PEAZs"
ADDRESS_UPDATE_FILE_ID = "1t1ORfuuHfW3VZ0yXTiIaaBgHzYF8MDwd"


# --- 데이터 처리용 상수 ---
SM_QTY_COL_TREND = '잔량(박스)'
SM_WGT_COL_TREND = '잔량(Kg)'

REPORT_LOCATION_MAP_TREND = {'신갈냉동': '신갈', '선왕CH4층': '선왕', '신갈김형제': '김형제', '신갈상이품/작업': '상이품', '케이미트스토어': '스토어'}
TARGET_SM_LOCATIONS_FOR_TREND = ['신갈냉동', '선왕CH4층', '신갈김형제', '신갈상이품/작업', '케이미트스토어']
REPORT_ROW_ORDER_TREND = ['신갈', '선왕', '김형제', '상이품', '스토어']
# 입고 관련 상수
PURCHASE_DATE_COL = '매입일자'; PURCHASE_CODE_COL = '코드'; PURCHASE_CUSTOMER_COL = '거래처명' # 원본 파일 컬럼명 유지
PURCHASE_PROD_CODE_COL = '상품코드'; PURCHASE_PROD_NAME_COL = '상 품 명'; PURCHASE_LOCATION_COL = '지 점 명'
PURCHASE_QTY_BOX_COL = 'Box'; PURCHASE_QTY_KG_COL = 'Kg'
PURCHASE_LOG_SHEET_NAME = 'p-list' # 원본 파일 시트명 유지
# 출고 관련 상수
SALES_DATE_COL = '매출일자'; SALES_PROD_CODE_COL = '상품코드'; SALES_PROD_NAME_COL = '상  품  명' # 원본 파일 컬럼명 유지
SALES_QTY_BOX_COL = '수량(Box)'; SALES_QTY_KG_COL = '수량(Kg)'; SALES_LOCATION_COL = '지점명'
SALES_LOG_SHEET_NAME = 's-list' # 원본 파일 시트명 유지

CURRENT_STOCK_PROD_CODE_COL = '상품코드'; CURRENT_STOCK_PROD_NAME_COL = '상품명'
CURRENT_STOCK_QTY_COL = SM_QTY_COL_TREND
CURRENT_STOCK_WGT_COL = SM_WGT_COL_TREND
CURRENT_STOCK_LOCATION_COL = '지점명'
SUMMARY_TABLE_LOCATIONS = ['신갈냉동', '선왕CH4층', '신갈김형제', '신갈상이품/작업', '케이미트스토어']


# --- Google Drive 파일 다운로드 헬퍼 함수 ---
@st.cache_data(ttl=300)
def download_excel_from_drive(file_id, file_name_for_error_msg=""):
    if drive_service is None: # 이 함수가 호출되기 전에 drive_service가 초기화되어야 함
        st.error(f"오류: 파일 다운로드 시도 중 Google Drive 서비스가 초기화되지 않았습니다 ({file_name_for_error_msg}). 앱 상단 인증 부분을 확인하세요.")
        if 'drive_service_none_in_download' not in st.session_state:
            st.session_state.drive_service_none_in_download = True
        return None
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except HttpError as error:
        session_key_download_error = f"download_error_displayed_{file_id}"
        if session_key_download_error not in st.session_state :
            st.sidebar.warning(f"경고: 파일(ID: {file_id}, 이름: {file_name_for_error_msg}) 다운로드 실패: {error.resp.status} - {error._get_reason()}. 파일 공유 설정을 확인하세요.")
            st.session_state[session_key_download_error] = True
        return None
    except Exception as e:
        session_key_general_error = f"general_error_displayed_{file_id}"
        if session_key_general_error not in st.session_state:
            st.sidebar.warning(f"경고: 파일(ID: {file_id}, 이름: {file_name_for_error_msg}) 처리 중 예외 발생: {e}")
            st.session_state[session_key_general_error] = True
        return None

# --- 데이터 로딩 함수 (Drive API 사용) ---
@st.cache_data(ttl=300)
def get_all_available_sheet_dates_from_excel(file_id, file_name_for_error_msg="SM재고현황.xlsx"):
    fh = download_excel_from_drive(file_id, file_name_for_error_msg)
    if fh is None: return []
    try:
        xls = pd.ExcelFile(fh)
        sheet_names = xls.sheet_names
        valid_dates = []
        for name in sheet_names:
            try:
                dt_obj = datetime.datetime.strptime(name, "%Y%m%d").date()
                valid_dates.append(dt_obj)
            except ValueError:
                continue
        valid_dates.sort(reverse=True)
        return valid_dates
    except Exception as e:
        st.sidebar.warning(f"경고: '{file_name_for_error_msg}' 엑셀 파일 시트 목록 조회 중 오류: {e}")
        return []

@st.cache_data(ttl=300)
def load_sm_data_from_excel(file_id, date_strings_yyyymmdd_list, file_name_for_error_msg="SM재고현황.xlsx"):
    if not date_strings_yyyymmdd_list: return None
    fh = download_excel_from_drive(file_id, file_name_for_error_msg)
    if fh is None: return None

    all_data = []
    try:
        xls = pd.ExcelFile(fh)
        available_sheets = xls.sheet_names
        for date_str in date_strings_yyyymmdd_list:
            if date_str in available_sheets:
                try:
                    df_sheet = pd.read_excel(xls, sheet_name=date_str, header=0)
                    df_sheet.dropna(how='all', inplace=True)
                    if df_sheet.empty: continue

                    required_cols = ['지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND]
                    if not all(col in df_sheet.columns for col in required_cols):
                        missing = [col for col in required_cols if col not in df_sheet.columns]
                        st.sidebar.warning(f"경고: '{file_name_for_error_msg}' 파일의 '{date_str}' 시트에 필수 컬럼 {missing} 중 일부가 누락되어 해당 시트를 건너뜁니다.")
                        continue

                    df_sheet_copy = df_sheet.copy()
                    df_sheet_copy['날짜'] = pd.to_datetime(date_str, format='%Y%m%d')
                    df_processed_sheet = df_sheet_copy[['날짜', '지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND]].copy()

                    for col in [SM_QTY_COL_TREND, SM_WGT_COL_TREND]:
                        df_processed_sheet[col] = pd.to_numeric(df_processed_sheet[col], errors='coerce').fillna(0)

                    df_processed_sheet['지점명'] = df_processed_sheet['지점명'].astype(str).str.strip()
                    df_processed_sheet['날짜'] = pd.to_datetime(df_processed_sheet['날짜']).dt.normalize()
                    all_data.append(df_processed_sheet)
                except Exception as e_sheet:
                    st.sidebar.warning(f"경고: '{file_name_for_error_msg}' 파일의 시트 '{date_str}' 처리 중 오류: {e_sheet}")
                    continue
        if not all_data: return None
        return pd.concat(all_data, ignore_index=True)
    except Exception as e_main:
        st.sidebar.error(f"오류: '{file_name_for_error_msg}' 엑셀 파일 로딩 중 주요 오류 발생: {e_main}")
        return None

@st.cache_data(ttl=300)
def get_latest_date_from_log(file_id, sheet_name, date_col, file_name_for_error_msg=""): # file_name_for_error_msg에 "입고내역.xlsx" 또는 "출고내역.xlsx" 전달
    fh = download_excel_from_drive(file_id, file_name_for_error_msg)
    if fh is None: return None
    try:
        df = pd.read_excel(fh, sheet_name=sheet_name, header=0)
        df.dropna(subset=[date_col], how='all', inplace=True)
        if df.empty or date_col not in df.columns: return None

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df.dropna(subset=[date_col], inplace=True)
        if df.empty: return None

        return df[date_col].max().date()
    except Exception as e:
        st.sidebar.warning(f"경고: '{file_name_for_error_msg}' ({sheet_name} 시트) 최신 날짜 조회 중 오류: {e}")
        return None

@st.cache_data(ttl=300)
def load_daily_log_data_for_period_from_excel(file_id, sheet_name, date_col, location_col, qty_box_col, qty_kg_col, start_date, end_date, is_purchase_log=False, file_name_for_error_msg=""): # file_name_for_error_msg에 "입고내역.xlsx" 또는 "출고내역.xlsx" 전달
    fh = download_excel_from_drive(file_id, file_name_for_error_msg)
    if fh is None: return pd.DataFrame()
    try:
        df = pd.read_excel(fh, sheet_name=sheet_name, header=0)
        df.dropna(how='all', inplace=True)
        if df.empty: return pd.DataFrame()

        if is_purchase_log:
            ffill_cols = [date_col, location_col, PURCHASE_CODE_COL, PURCHASE_CUSTOMER_COL]
            for col_to_ffill in ffill_cols:
                if col_to_ffill in df.columns:
                    df[col_to_ffill] = df[col_to_ffill].ffill()
                elif col_to_ffill == date_col or col_to_ffill == location_col:
                    st.sidebar.warning(f"경고: '{file_name_for_error_msg}' ({sheet_name}) 입고 로그에 필수 ffill 컬럼({col_to_ffill}) 누락.")
                    return pd.DataFrame()

        required_cols_log = [date_col, location_col, qty_box_col, qty_kg_col]
        if not all(col in df.columns for col in required_cols_log):
            missing_log_cols = [col for col in required_cols_log if col not in df.columns]
            st.sidebar.warning(f"경고: '{file_name_for_error_msg}' ({sheet_name})에 필수 컬럼 {missing_log_cols} 누락.")
            return pd.DataFrame()

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.normalize()
        df.dropna(subset=[date_col], inplace=True)
        if df.empty: return pd.DataFrame()

        mask = (df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)
        df_period = df.loc[mask].copy()

        if df_period.empty: return pd.DataFrame()

        for col in [qty_box_col, qty_kg_col]:
            df_period[col] = pd.to_numeric(df_period[col], errors='coerce').fillna(0)
        df_period[location_col] = df_period[location_col].astype(str).str.strip()

        daily_summary = df_period.groupby([df_period[date_col].dt.date, location_col]).agg(
            TotalQtyBox=(qty_box_col, 'sum'),
            TotalQtyKg=(qty_kg_col, 'sum')
        ).reset_index()
        daily_summary.rename(columns={date_col: '날짜'}, inplace=True)
        return daily_summary
    except Exception as e:
        st.sidebar.error(f"오류: '{file_name_for_error_msg}' ({sheet_name} 시트) 일별 기간 데이터 로딩 중 오류: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_log_data_for_period_from_excel(file_id, sheet_name, date_col, qty_kg_col, location_col, start_date, end_date, is_purchase_log=False, file_name_for_error_msg=""): # file_name_for_error_msg에 "입고내역.xlsx" 또는 "출고내역.xlsx" 전달
    fh = download_excel_from_drive(file_id, file_name_for_error_msg)
    if fh is None: return pd.DataFrame()
    try:
        df = pd.read_excel(fh, sheet_name=sheet_name, header=0)
        df.dropna(how='all', inplace=True)
        if df.empty: return pd.DataFrame()

        if is_purchase_log:
            ffill_cols = [date_col, location_col, PURCHASE_CODE_COL, PURCHASE_CUSTOMER_COL]
            for col_to_ffill in ffill_cols:
                if col_to_ffill in df.columns: df[col_to_ffill] = df[col_to_ffill].ffill()
                elif col_to_ffill == date_col or col_to_ffill == location_col:
                    st.sidebar.warning(f"경고: '{file_name_for_error_msg}' ({sheet_name}) 입고 로그(월별)에 필수 ffill 컬럼({col_to_ffill}) 누락.")
                    return pd.DataFrame()

        if date_col not in df.columns or qty_kg_col not in df.columns:
            st.sidebar.warning(f"경고: '{file_name_for_error_msg}' ({sheet_name})에 필수 컬럼 ({date_col} 또는 {qty_kg_col}) 누락.")
            return pd.DataFrame()

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.normalize()
        df.dropna(subset=[date_col], inplace=True)
        df[qty_kg_col] = pd.to_numeric(df[qty_kg_col], errors='coerce').fillna(0)

        mask = (df[date_col].dt.date >= start_date) & (df[date_col].dt.date <= end_date)
        df_period = df.loc[mask].copy()
        if df_period.empty: return pd.DataFrame()

        df_period['월'] = df_period[date_col].dt.strftime('%Y-%m')
        monthly_sum = df_period.groupby('월')[qty_kg_col].sum().reset_index()
        monthly_sum.rename(columns={qty_kg_col: '중량(Kg)'}, inplace=True)
        return monthly_sum
    except Exception as e:
        st.sidebar.error(f"오류: '{file_name_for_error_msg}' ({sheet_name} 시트) 기간 데이터(월별) 로딩 중 오류: {e}")
        return pd.DataFrame()

# --- 페이지 렌더링 함수 정의 ---
def render_daily_trend_page_layout():
    now_time = datetime.datetime.now()
    current_time_str = now_time.strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.1rem;'>📊 데이터 분석 대시보드</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center; margin-top: 0.1rem; font-size: 0.9em;'>현재 시간: {current_time_str}</p>", unsafe_allow_html=True)
    st.markdown("---", unsafe_allow_html=True)

    all_available_dates_desc = get_all_available_sheet_dates_from_excel(SM_FILE_ID, "SM재고현황.xlsx")
    dates_for_report = []

    if not all_available_dates_desc:
        st.warning("경고: 'SM재고현황.xlsx' 파일에서 사용 가능한 날짜 형식의 시트를 찾을 수 없습니다. 파일 내용 및 시트 이름을 확인하세요.")
    else:
        today = datetime.date.today()
        latest_anchor_date = next((dt for dt in all_available_dates_desc if dt <= today), None)

        if latest_anchor_date is None:
            st.warning(f"경고: 오늘({today.strftime('%Y-%m-%d')}) 또는 그 이전 날짜에 대한 데이터를 'SM재고현황.xlsx'에서 찾을 수 없습니다. 가장 최근 데이터로 리포트를 생성합니다.")
            latest_anchor_date = all_available_dates_desc[0]

        start_index = all_available_dates_desc.index(latest_anchor_date)
        end_index = min(start_index + 7, len(all_available_dates_desc))
        dates_for_report = all_available_dates_desc[start_index:end_index][:7]

        if dates_for_report:
            dates_for_report.sort()
            st.info(f"분석 기간 ({len(dates_for_report)}일 데이터): {dates_for_report[0].strftime('%Y-%m-%d')} ~ {dates_for_report[-1].strftime('%Y-%m-%d')}")
        else:
            st.warning("경고: 리포트에 사용할 날짜를 선정하지 못했습니다. 'SM재고현황.xlsx' 파일 데이터를 확인하세요.")

    report_dates_pd = pd.to_datetime(dates_for_report).normalize() if dates_for_report else pd.DatetimeIndex([])
    report_date_str_list_yyyymmdd = [d.strftime("%Y%m%d") for d in dates_for_report]

    df_sm_trend_raw = None
    if report_date_str_list_yyyymmdd:
        df_sm_trend_raw = load_sm_data_from_excel(SM_FILE_ID, report_date_str_list_yyyymmdd, "SM재고현황.xlsx")

    daily_location_summary = None
    if df_sm_trend_raw is not None and not df_sm_trend_raw.empty:
        df_sm_trend_filtered = df_sm_trend_raw[df_sm_trend_raw['지점명'].isin(TARGET_SM_LOCATIONS_FOR_TREND)].copy()
        if not df_sm_trend_filtered.empty:
            df_sm_trend_filtered['창고명'] = df_sm_trend_filtered['지점명'].map(REPORT_LOCATION_MAP_TREND)
            daily_location_summary = df_sm_trend_filtered.groupby(['날짜', '창고명'])[[SM_QTY_COL_TREND, SM_WGT_COL_TREND]].sum().reset_index()

    title_style = "<h3 style='margin-bottom:0.2rem; margin-top:0.5rem; font-size:1.25rem;'>"

    # --- 첫 번째 행: 3개 항목 ---
    row1_cols = st.columns(3)

    with row1_cols[0]:
        st.markdown(f"{title_style}1. 일별 재고량({SM_QTY_COL_TREND}) 추이</h3>", unsafe_allow_html=True)
        if daily_location_summary is not None and not daily_location_summary.empty and not report_dates_pd.empty:
            try:
                daily_location_summary['날짜'] = pd.to_datetime(daily_location_summary['날짜']).dt.normalize()
                chart_pivot_qty_raw = daily_location_summary.pivot_table(index='날짜', columns='창고명', values=SM_QTY_COL_TREND)
                chart_pivot_qty_final = chart_pivot_qty_raw.reindex(index=report_dates_pd, columns=REPORT_ROW_ORDER_TREND).fillna(0)
                st.line_chart(chart_pivot_qty_final, use_container_width=True, height=250)
            except Exception as e_chart1:
                st.write(f"재고량 추이 차트 생성 오류.")
                st.line_chart(pd.DataFrame(index=report_dates_pd, columns=REPORT_ROW_ORDER_TREND).fillna(0), use_container_width=True, height=250)
        elif dates_for_report:
            st.write("표시할 그래프 데이터가 없습니다.")
            st.line_chart(pd.DataFrame(index=report_dates_pd, columns=REPORT_ROW_ORDER_TREND).fillna(0), use_container_width=True, height=250)
        else:
            st.write("데이터 로드 불가 또는 분석 기간 없음")


    with row1_cols[1]:
        st.markdown(f"{title_style}2. 재고 비중 ({SM_QTY_COL_TREND})</h3>", unsafe_allow_html=True)
        if daily_location_summary is not None and not daily_location_summary.empty and not report_dates_pd.empty:
            latest_report_date_ts = report_dates_pd[-1]
            df_latest_day_stock = daily_location_summary[daily_location_summary['날짜'] == latest_report_date_ts]
            if not df_latest_day_stock.empty and df_latest_day_stock[SM_QTY_COL_TREND].sum() > 0:
                fig_qty = px.pie(df_latest_day_stock, names='창고명', values=SM_QTY_COL_TREND, hole=.4, title=f"{latest_report_date_ts.strftime('%m/%d')} (박스)")
                fig_qty.update_traces(textposition='inside', textinfo='percent+label', pull=[0.05 if qty > 0 else 0 for qty in df_latest_day_stock[SM_QTY_COL_TREND]])
                fig_qty.update_layout(showlegend=False, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20), height=280)
                st.plotly_chart(fig_qty, use_container_width=True)
            elif dates_for_report: st.write(f"{latest_report_date_ts.strftime('%m/%d')} 데이터 없음")
            else: st.write("데이터 없음")
        elif dates_for_report: st.write("최신일자 데이터 없음")
        else: st.write("데이터 로드 불가 또는 분석 기간 없음")

    with row1_cols[2]:
        st.markdown(f"{title_style}3. 재고 비중 ({SM_WGT_COL_TREND})</h3>", unsafe_allow_html=True)
        if daily_location_summary is not None and not daily_location_summary.empty and not report_dates_pd.empty:
            latest_report_date_ts = report_dates_pd[-1]
            df_latest_day_stock_wgt = daily_location_summary[daily_location_summary['날짜'] == latest_report_date_ts]
            if not df_latest_day_stock_wgt.empty and df_latest_day_stock_wgt[SM_WGT_COL_TREND].sum() > 0:
                fig_wgt = px.pie(df_latest_day_stock_wgt, names='창고명', values=SM_WGT_COL_TREND, hole=.4, title=f"{latest_report_date_ts.strftime('%m/%d')} (Kg)")
                fig_wgt.update_traces(textposition='inside', textinfo='percent+label', pull=[0.05 if wgt > 0 else 0 for wgt in df_latest_day_stock_wgt[SM_WGT_COL_TREND]])
                fig_wgt.update_layout(showlegend=False, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20), height=280)
                st.plotly_chart(fig_wgt, use_container_width=True)
            elif dates_for_report: st.write(f"{latest_report_date_ts.strftime('%m/%d')} 데이터 없음")
            else: st.write("데이터 없음")
        elif dates_for_report: st.write("최신일자 데이터 없음")
        else: st.write("데이터 로드 불가 또는 분석 기간 없음")

    st.markdown("---")

    # --- 두 번째 행 (칸 4): 일별 창고 재고량 표 (가로 전체) ---
    st.markdown(f"{title_style}4. 일별 창고 재고량 ({SM_QTY_COL_TREND}/{SM_WGT_COL_TREND})</h3>", unsafe_allow_html=True); st.caption("표가 길 경우 스크롤하세요.")
    if daily_location_summary is not None and not daily_location_summary.empty and not report_dates_pd.empty:
        try:
            table_pivot_qty = daily_location_summary.pivot_table(index='창고명', columns='날짜', values=SM_QTY_COL_TREND, fill_value=0).reindex(index=REPORT_ROW_ORDER_TREND, columns=report_dates_pd, fill_value=0)
            table_pivot_wgt = daily_location_summary.pivot_table(index='창고명', columns='날짜', values=SM_WGT_COL_TREND, fill_value=0).reindex(index=REPORT_ROW_ORDER_TREND, columns=report_dates_pd, fill_value=0)

            qty_diff = pd.DataFrame(index=table_pivot_qty.index, columns=table_pivot_qty.columns); daily_qty_totals = table_pivot_qty.sum(axis=0)
            daily_wgt_totals = table_pivot_wgt.sum(axis=0); total_qty_diff = pd.Series(dtype='float64', index=daily_qty_totals.index)

            if len(table_pivot_qty.columns) > 1: qty_diff = table_pivot_qty.diff(axis=1)
            if len(daily_qty_totals.index) > 1: total_qty_diff = daily_qty_totals.diff()

            combined_table = pd.DataFrame(index=table_pivot_qty.index, columns=table_pivot_qty.columns, dtype=object)
            for date_col_ts in combined_table.columns: # date_col_ts는 Timestamp
                qty_series = table_pivot_qty[date_col_ts]; wgt_series = table_pivot_wgt[date_col_ts]; diff_series = qty_diff.get(date_col_ts)
                cell_strings = []
                for warehouse in combined_table.index:
                    qty_val = qty_series.get(warehouse, 0); wgt_val = wgt_series.get(warehouse, 0); diff_val = diff_series.get(warehouse, None) if diff_series is not None else None
                    base_string = f"{qty_val:,.0f} / {wgt_val:,.1f} Kg"; indicator = ""
                    if qty_val == 0 and wgt_val == 0: cell_strings.append("-")
                    else:
                        if pd.notnull(diff_val) and len(table_pivot_qty.columns) > 1:
                            if diff_val > 0.01: indicator = "🔺 "
                            elif diff_val < -0.01: indicator = "▼ "
                        cell_strings.append(f"{indicator}{base_string}")
                combined_table[date_col_ts] = cell_strings

            total_row_data = {}
            for date_col_ts in table_pivot_qty.columns: # date_col_ts는 Timestamp
                total_qty_val = daily_qty_totals.get(date_col_ts, 0); total_wgt_val = daily_wgt_totals.get(date_col_ts, 0); total_diff_val = total_qty_diff.get(date_col_ts, None)
                base_total_string = f"{total_qty_val:,.0f} / {total_wgt_val:,.1f} Kg"; total_indicator = ""
                if total_qty_val == 0 and total_wgt_val == 0: total_row_data[date_col_ts] = "-"
                else:
                    if pd.notnull(total_diff_val) and len(daily_qty_totals.index) > 1:
                        if total_diff_val > 0.01: total_indicator = "🔺 "
                        elif total_diff_val < -0.01: total_indicator = "▼ "
                    total_row_data[date_col_ts] = f"{total_indicator}{base_total_string}"
            combined_table.loc['합계'] = pd.Series(total_row_data)

            # 날짜 컬럼에 요일 추가 (MM/DD(요일) 형식)
            combined_table.columns = [ts.strftime('%m/%d') + f"({KOREAN_DAYS[ts.weekday()]})" for ts in combined_table.columns]

            combined_table_display = combined_table.reindex(REPORT_ROW_ORDER_TREND + ['합계'])
            st.dataframe(combined_table_display.reset_index().rename(columns={'index': '창고명'}), hide_index=True, use_container_width=True, height=300)
        except Exception as e_table:
            st.error(f"표 데이터 생성 중 오류: {e_table}")
            # traceback.print_exc() # 개발 시 상세 오류 확인
    elif dates_for_report:
        st.write("표시할 테이블 데이터가 없습니다.")
        if not report_dates_pd.empty:
            empty_table_cols = [ts.strftime('%m/%d') + f"({KOREAN_DAYS[ts.weekday()]})" for ts in report_dates_pd]
            empty_table_data = {col_name: ['-'] * (len(REPORT_ROW_ORDER_TREND) + 1) for col_name in empty_table_cols}
            empty_table_df = pd.DataFrame(empty_table_data, index=REPORT_ROW_ORDER_TREND + ['합계']); empty_table_df.index.name = '창고명'
            st.dataframe(empty_table_df.reset_index(), hide_index=True, use_container_width=True, height=300)
    else:
        st.write("데이터 로드 불가 또는 분석 기간 없음")

    st.markdown("---")

    # --- 세 번째 행 (칸 5): 최근 7일 일별 입고/출고 현황 (가로 전체) ---
    st.markdown(f"{title_style}5. 최근 7일 일별 입고/출고 현황</h3>", unsafe_allow_html=True)

    latest_purchase_date = get_latest_date_from_log(PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, "입고내역.xlsx") # 용어 변경
    latest_sales_date = get_latest_date_from_log(SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, "출고내역.xlsx")       # 용어 변경

    overall_latest_date = None
    if latest_purchase_date and latest_sales_date: overall_latest_date = max(latest_purchase_date, latest_sales_date)
    elif latest_purchase_date: overall_latest_date = latest_purchase_date
    elif latest_sales_date: overall_latest_date = latest_sales_date

    if overall_latest_date:
        end_date_7day = overall_latest_date
        start_date_7day = end_date_7day - datetime.timedelta(days=6)
        period_caption = f"기간: {start_date_7day.strftime('%Y-%m-%d')} ~ {end_date_7day.strftime('%Y-%m-%d')}"
        actual_7day_date_range = [start_date_7day + datetime.timedelta(days=i) for i in range(7)] # datetime.date 객체 리스트

        log_cols = st.columns(2)
        with log_cols[0]:
            st.markdown("<h4 style='font-size:1.0rem; margin-bottom:0.1rem;'>일별 입고 현황 (Box/Kg)</h4>", unsafe_allow_html=True) # 용어 변경
            st.caption(period_caption)
            df_purchase_daily_raw = load_daily_log_data_for_period_from_excel(
                PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME,
                PURCHASE_DATE_COL, PURCHASE_LOCATION_COL, PURCHASE_QTY_BOX_COL, PURCHASE_QTY_KG_COL,
                start_date_7day, end_date_7day,
                is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx" # 용어 변경
            )

            if df_purchase_daily_raw is not None and not df_purchase_daily_raw.empty:
                purchase_pivot_box = df_purchase_daily_raw.pivot_table(index=PURCHASE_LOCATION_COL, columns='날짜', values='TotalQtyBox', fill_value=0)
                purchase_pivot_kg = df_purchase_daily_raw.pivot_table(index=PURCHASE_LOCATION_COL, columns='날짜', values='TotalQtyKg', fill_value=0)
                purchase_pivot_box = purchase_pivot_box.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)
                purchase_pivot_kg = purchase_pivot_kg.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)

                purchase_combined_table = pd.DataFrame(index=purchase_pivot_box.index, columns=purchase_pivot_box.columns, dtype=object) # 컬럼은 datetime.date 객체
                daily_purchase_totals_box = purchase_pivot_box.sum(axis=0)
                daily_purchase_totals_kg = purchase_pivot_kg.sum(axis=0)

                for date_col_obj in purchase_combined_table.columns: # date_col_obj는 datetime.date 객체
                    for loc in purchase_combined_table.index:
                        box = purchase_pivot_box.loc[loc, date_col_obj]
                        kg = purchase_pivot_kg.loc[loc, date_col_obj]
                        purchase_combined_table.loc[loc, date_col_obj] = f"{box:,.0f} / {kg:,.1f}" if not (box == 0 and kg == 0) else "-"

                total_row_data_p = {date_obj: f"{daily_purchase_totals_box.get(date_obj, 0):,.0f} / {daily_purchase_totals_kg.get(date_obj, 0):,.1f}"
                                    if not (daily_purchase_totals_box.get(date_obj, 0) == 0 and daily_purchase_totals_kg.get(date_obj, 0) == 0) else "-"
                                    for date_obj in purchase_combined_table.columns}
                purchase_combined_table.loc['합계'] = pd.Series(total_row_data_p)
                # 날짜 컬럼에 요일 추가 (MM/DD(요일) 형식)
                purchase_combined_table.columns = [d.strftime('%m/%d') + f"({KOREAN_DAYS[d.weekday()]})" for d in purchase_combined_table.columns]
                st.dataframe(purchase_combined_table.reset_index().rename(columns={'index': '지점명'}), hide_index=True, use_container_width=True, height=250)
            else:
                st.write("해당 기간 입고 데이터가 없습니다.") # 용어 변경

        with log_cols[1]:
            st.markdown("<h4 style='font-size:1.0rem; margin-bottom:0.1rem;'>일별 출고 현황 (Box/Kg)</h4>", unsafe_allow_html=True) # 용어 변경
            st.caption(period_caption)
            df_sales_daily_raw = load_daily_log_data_for_period_from_excel(
                SALES_FILE_ID, SALES_LOG_SHEET_NAME,
                SALES_DATE_COL, SALES_LOCATION_COL, SALES_QTY_BOX_COL, SALES_QTY_KG_COL,
                start_date_7day, end_date_7day,
                file_name_for_error_msg="출고내역.xlsx" # 용어 변경
            )

            if df_sales_daily_raw is not None and not df_sales_daily_raw.empty:
                sales_pivot_box = df_sales_daily_raw.pivot_table(index=SALES_LOCATION_COL, columns='날짜', values='TotalQtyBox', fill_value=0)
                sales_pivot_kg = df_sales_daily_raw.pivot_table(index=SALES_LOCATION_COL, columns='날짜', values='TotalQtyKg', fill_value=0)
                sales_pivot_box = sales_pivot_box.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)
                sales_pivot_kg = sales_pivot_kg.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)

                sales_combined_table = pd.DataFrame(index=sales_pivot_box.index, columns=sales_pivot_box.columns, dtype=object) # 컬럼은 datetime.date 객체
                daily_sales_totals_box = sales_pivot_box.sum(axis=0)
                daily_sales_totals_kg = sales_pivot_kg.sum(axis=0)

                for date_col_obj in sales_combined_table.columns: # date_col_obj는 datetime.date 객체
                    for loc in sales_combined_table.index:
                        box = sales_pivot_box.loc[loc, date_col_obj]
                        kg = sales_pivot_kg.loc[loc, date_col_obj]
                        sales_combined_table.loc[loc, date_col_obj] = f"{box:,.0f} / {kg:,.1f}" if not (box == 0 and kg == 0) else "-"

                total_row_data_s = {date_obj: f"{daily_sales_totals_box.get(date_obj, 0):,.0f} / {daily_sales_totals_kg.get(date_obj, 0):,.1f}"
                                    if not (daily_sales_totals_box.get(date_obj, 0) == 0 and daily_sales_totals_kg.get(date_obj, 0) == 0) else "-"
                                    for date_obj in sales_combined_table.columns}
                sales_combined_table.loc['합계'] = pd.Series(total_row_data_s)
                # 날짜 컬럼에 요일 추가 (MM/DD(요일) 형식)
                sales_combined_table.columns = [d.strftime('%m/%d') + f"({KOREAN_DAYS[d.weekday()]})" for d in sales_combined_table.columns]
                st.dataframe(sales_combined_table.reset_index().rename(columns={'index': '지점명'}), hide_index=True, use_container_width=True, height=250)
            else:
                st.write("해당 기간 출고 데이터가 없습니다.") # 용어 변경
    else:
        st.write("입고/출고 데이터를 가져올 수 없습니다 (최신 날짜 정보 없음).") # 용어 변경

    st.markdown("---")

    # --- 네 번째 행 (칸 6): 전년 동기 중량 비교 (가로 전체) ---
    st.markdown(f"{title_style}6. 전년 동기 중량 비교 (Kg)</h3>", unsafe_allow_html=True)
    today = datetime.date.today()
    current_year_start = today.replace(month=1, day=1); current_year_end = today
    previous_year_start = current_year_start - relativedelta(years=1); previous_year_end = current_year_end - relativedelta(years=1)
    st.caption(f"기간: 올해({current_year_start.strftime('%y/%m/%d')}~{current_year_end.strftime('%y/%m/%d')}) vs 작년({previous_year_start.strftime('%y/%m/%d')}~{previous_year_end.strftime('%y/%m/%d')})")

    df_sales_cy = load_log_data_for_period_from_excel(SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL, current_year_start, current_year_end, file_name_for_error_msg="출고내역.xlsx") # 용어 변경
    df_sales_py = load_log_data_for_period_from_excel(SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL, previous_year_start, previous_year_end, file_name_for_error_msg="출고내역.xlsx") # 용어 변경
    df_purchase_cy = load_log_data_for_period_from_excel(PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, PURCHASE_QTY_KG_COL, PURCHASE_LOCATION_COL, current_year_start, current_year_end, is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx") # 용어 변경
    df_purchase_py = load_log_data_for_period_from_excel(PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, PURCHASE_QTY_KG_COL, PURCHASE_LOCATION_COL, previous_year_start, previous_year_end, is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx") # 용어 변경

    def prepare_comparison_df(df_cy, df_py, name_prefix): # name_prefix에 "입고" 또는 "출고" 전달
        df_list = []
        if df_cy is not None and not df_cy.empty:
            df_cy_copy = df_cy.copy(); df_cy_copy['구분'] = f'{name_prefix} (올해)'; df_list.append(df_cy_copy)
        if df_py is not None and not df_py.empty:
            df_py_copy = df_py.copy()
            df_py_copy['월'] = pd.to_datetime(df_py_copy['월']).apply(lambda x: x.replace(year=today.year)).dt.strftime('%Y-%m')
            df_py_copy['구분'] = f'{name_prefix} (작년)'; df_list.append(df_py_copy)

        if not df_list: return pd.DataFrame(columns=['월', '중량(Kg)', '구분'])

        combined = pd.concat(df_list)
        return combined

    def plot_comparison_chart(df_combined, title): # title에 "월별 입고 중량 비교" 또는 "월별 출고 중량 비교" 전달
        if df_combined.empty or '중량(Kg)' not in df_combined.columns or df_combined['중량(Kg)'].sum() == 0 :
            st.write(f"{title}: 표시할 데이터가 없습니다."); return

        df_combined_sorted = df_combined.copy()
        df_combined_sorted['월_dt'] = pd.to_datetime(df_combined_sorted['월'])
        df_combined_sorted = df_combined_sorted.sort_values('월_dt')

        fig = px.line(df_combined_sorted, x='월', y='중량(Kg)', color='구분', markers=True,
                        title=title, labels={'월': '월', '중량(Kg)': '총 중량(Kg)'})
        fig.update_layout(height=280, margin=dict(t=30, b=30, l=0, r=0), legend_title_text='')
        st.plotly_chart(fig, use_container_width=True)

    comparison_cols = st.columns(2)
    with comparison_cols[0]:
        df_purchase_compare = prepare_comparison_df(df_purchase_cy, df_purchase_py, "입고") # 용어 변경
        plot_comparison_chart(df_purchase_compare, "월별 입고 중량 비교") # 용어 변경

    with comparison_cols[1]:
        df_sales_compare = prepare_comparison_df(df_sales_cy, df_sales_py, "출고") # 용어 변경
        plot_comparison_chart(df_sales_compare, "월별 출고 중량 비교") # 용어 변경

# --- 앱 실행 부분 ---
# SERVICE_ACCOUNT_LOADED 와 drive_service 가 정상적으로 설정되었는지 다시 한번 확인 (디버깅 코드에서 이미 처리하지만, 최종 관문)
if SERVICE_ACCOUNT_LOADED and drive_service is not None:
    st.success("최종 인증 확인: Google Drive 서비스 사용 준비 완료. 페이지 렌더링을 시작합니다.")
    render_daily_trend_page_layout()
    st.sidebar.markdown("---")
else:
    # 이 부분은 디버깅 코드에서 st.stop()을 만나지 않았지만, 여전히 인증에 실패한 경우를 대비
    st.error("최종 인증 확인 실패: 페이지를 렌더링할 수 없습니다. 앱 상단의 디버깅 메시지나 Streamlit Cloud 로그를 확인하세요.")
    # 이미 critical_auth_error_displayed 세션 상태가 설정되었을 것이므로 추가 설정 불필요
    pass
