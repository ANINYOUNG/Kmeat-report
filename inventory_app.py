# inventory_app.py (메인 UI 복원 및 Cloud 연동)

import streamlit as st

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="데이터 분석 대시보드", layout="wide", initial_sidebar_state="expanded")

# --- 2. 기본 라이브러리 임포트 ---
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os
import traceback
import plotly.express as px
import json
import io

# --- 3. Google Drive API 관련 라이브러리 임포트 ---
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# --- 4. common_utils.py 에서 함수 및 상수 가져오기 (선택 사항) ---
# 만약 common_utils.py에 아래 함수들이 정의되어 있다면 그곳에서 import 합니다.
# 여기서는 이 파일 내에 해당 함수들을 정의하거나 Cloud용으로 수정한다고 가정합니다.
# from common_utils import (
#     download_excel_from_drive_as_bytes,
#     get_all_available_sheet_dates_from_bytes,
#     # 필요한 다른 함수들...
# )

# --- 한국어 요일 리스트 ---
KOREAN_DAYS = ['월', '화', '수', '목', '금', '토', '일']

# --- Google API 인증 및 Drive 서비스 클라이언트 생성 ---
drive_service = None
SERVICE_ACCOUNT_LOADED = False

IS_CLOUD_ENVIRONMENT = "google_creds_json" in st.secrets

if IS_CLOUD_ENVIRONMENT:
    try:
        creds_json_str = st.secrets["google_creds_json"]
        creds_dict = json.loads(creds_json_str)
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        drive_service = build('drive', 'v3', credentials=creds)
        SERVICE_ACCOUNT_LOADED = True
    except Exception as e_secrets:
        st.sidebar.error(f"클라우드 Secrets 인증 중 오류: {e_secrets}")
        drive_service = None
        SERVICE_ACCOUNT_LOADED = False
else:
    SERVICE_ACCOUNT_FILE_PATH = "YOUR_LOCAL_SERVICE_ACCOUNT_FILE_PATH.json" # 실제 로컬 경로로 수정 필요
    if os.path.exists(SERVICE_ACCOUNT_FILE_PATH):
        try:
            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE_PATH, scopes=scopes)
            drive_service = build('drive', 'v3', credentials=creds)
            SERVICE_ACCOUNT_LOADED = True
        except Exception as e_local:
            st.sidebar.error(f"로컬 키 파일 인증 중 오류: {e_local}")
            drive_service = None
            SERVICE_ACCOUNT_LOADED = False
    else:
        st.sidebar.warning(f"로컬: 서비스 계정 키 파일 없음: {SERVICE_ACCOUNT_FILE_PATH}")
        drive_service = None
        SERVICE_ACCOUNT_LOADED = False

if SERVICE_ACCOUNT_LOADED and drive_service is not None:
    if 'drive_service' not in st.session_state or st.session_state.get('drive_service') is None:
        st.session_state['drive_service'] = drive_service
        st.sidebar.success("Drive service가 메인 앱 세션에 성공적으로 저장됨!")
elif not SERVICE_ACCOUNT_LOADED or drive_service is None:
    st.sidebar.error("메인 앱: Drive service 초기화 실패 또는 인증 정보 없음!")
    if 'drive_service' in st.session_state:
        del st.session_state['drive_service']

# --- Google Drive 파일 ID 정의 ---
SM_FILE_ID = "1tRljdvOpp4fITaVEXvoL9mNveNg2qt4p"
PURCHASE_FILE_ID = "1AgKl29yQ80sTDszLql6oBnd9FnLWf8oR"
SALES_FILE_ID = "1h-V7kIoInXgGLll7YBW5V_uZdF3Q1PdY"
# ERP_FILE_ID = "1Lbtwenw8LcDaj94_J4kKTjoWQY7PEAZs" # 필요시 주석 해제
# ADDRESS_UPDATE_FILE_ID = "1t1ORfuuHfW3VZ0yXTiIaaBgHzYF8MDwd" # 필요시 주석 해제

# --- 데이터 처리용 상수 (원본 코드에서 가져옴) ---
SM_QTY_COL_TREND = '잔량(박스)'
SM_WGT_COL_TREND = '잔량(Kg)'
REPORT_LOCATION_MAP_TREND = {'신갈냉동': '신갈', '선왕CH4층': '선왕', '신갈김형제': '김형제', '신갈상이품/작업': '상이품', '케이미트스토어': '스토어'}
TARGET_SM_LOCATIONS_FOR_TREND = ['신갈냉동', '선왕CH4층', '신갈김형제', '신갈상이품/작업', '케이미트스토어']
REPORT_ROW_ORDER_TREND = ['신갈', '선왕', '김형제', '상이품', '스토어']
PURCHASE_DATE_COL = '매입일자'; PURCHASE_CODE_COL = '코드'; PURCHASE_CUSTOMER_COL = '거래처명'
PURCHASE_PROD_CODE_COL = '상품코드'; PURCHASE_PROD_NAME_COL = '상 품 명'; PURCHASE_LOCATION_COL = '지 점 명'
PURCHASE_QTY_BOX_COL = 'Box'; PURCHASE_QTY_KG_COL = 'Kg'
PURCHASE_LOG_SHEET_NAME = 'p-list'
SALES_DATE_COL = '매출일자'; SALES_PROD_CODE_COL = '상품코드'; SALES_PROD_NAME_COL = '상  품  명'
SALES_QTY_BOX_COL = '수량(Box)'; SALES_QTY_KG_COL = '수량(Kg)'; SALES_LOCATION_COL = '지점명'
SALES_LOG_SHEET_NAME = 's-list'
SUMMARY_TABLE_LOCATIONS = ['신갈냉동', '선왕CH4층', '신갈김형제', '신갈상이품/작업', '케이미트스토어']

# --- Google Drive 파일 다운로드 헬퍼 함수 (Cloud용) ---
# (common_utils.py에 있다면 거기서 import, 여기서는 이 파일 내에 정의)
@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg="Excel file"):
    if current_drive_service is None:
        st.error(f"오류: Google Drive 서비스가 초기화되지 않았습니다. ({file_name_for_error_msg} 다운로드 시도)")
        return None
    try:
        request = current_drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except HttpError as error:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 다운로드 실패: {error.resp.status} - {error._get_reason()}. 파일 공유 설정을 확인하세요.")
        return None
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 처리 중 예외 발생: {e}")
        return None

# --- 데이터 로딩 함수 (Drive API 사용 및 Cloud용으로 수정) ---
@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def get_all_available_sheet_dates_from_excel_drive(current_drive_service, file_id, file_name_for_error_msg="SM재고현황.xlsx"):
    fh = download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg)
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
        st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}) 엑셀 파일 시트 목록 조회 중 오류: {e}")
        return []

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_sm_data_from_excel_drive(current_drive_service, file_id, date_strings_yyyymmdd_list, file_name_for_error_msg="SM재고현황.xlsx"):
    if not date_strings_yyyymmdd_list: return None
    fh = download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg)
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
                        st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}) 파일의 '{date_str}' 시트에 필수 컬럼 {missing} 중 일부가 누락되어 해당 시트를 건너뜁니다.")
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
                    st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}) 파일의 시트 '{date_str}' 처리 중 오류: {e_sheet}")
                    continue
        if not all_data: return None
        return pd.concat(all_data, ignore_index=True)
    except Exception as e_main:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 엑셀 파일 로딩 중 주요 오류 발생: {e_main}")
        return None

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def get_latest_date_from_log_drive(current_drive_service, file_id, sheet_name, date_col, file_name_for_error_msg=""):
    fh = download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg)
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
        st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name}) 최신 날짜 조회 중 오류: {e}")
        return None

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_daily_log_data_for_period_from_excel_drive(current_drive_service, file_id, sheet_name, date_col, location_col, qty_box_col, qty_kg_col, start_date, end_date, is_purchase_log=False, file_name_for_error_msg=""):
    fh = download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg)
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
                    st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name}) 입고 로그에 필수 ffill 컬럼({col_to_ffill}) 누락.")
                    return pd.DataFrame()
        
        required_cols_log = [date_col, location_col, qty_box_col, qty_kg_col]
        if not all(col in df.columns for col in required_cols_log):
            missing_log_cols = [col for col in required_cols_log if col not in df.columns]
            st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name})에 필수 컬럼 {missing_log_cols} 누락.")
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
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name}) 일별 기간 데이터 로딩 중 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_log_data_for_period_from_excel_drive(current_drive_service, file_id, sheet_name, date_col, qty_kg_col, location_col, start_date, end_date, is_purchase_log=False, file_name_for_error_msg=""):
    fh = download_excel_from_drive_as_bytes(current_drive_service, file_id, file_name_for_error_msg)
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
                    st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name}) 입고 로그(월별)에 필수 ffill 컬럼({col_to_ffill}) 누락.")
                    return pd.DataFrame()

        if date_col not in df.columns or qty_kg_col not in df.columns:
            st.warning(f"경고: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name})에 필수 컬럼 ({date_col} 또는 {qty_kg_col}) 누락.")
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
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}, 시트: {sheet_name}) 기간 데이터(월별) 로딩 중 오류: {e}")
        return pd.DataFrame()

# --- 페이지 렌더링 함수 정의 (메인 페이지 UI 전체 복원) ---
def render_daily_trend_page_layout():
    # st.session_state에서 drive_service 가져오기
    current_drive_service = st.session_state.get('drive_service')
    if not current_drive_service:
        st.error("Google Drive 서비스에 연결되지 않았습니다. 앱 설정을 확인하거나 잠시 후 다시 시도해주세요.")
        st.stop()

    now_time = datetime.datetime.now()
    current_time_str = now_time.strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"<h1 style='text-align: center; margin-bottom: 0.1rem;'>📊 데이터 분석 대시보드 (메인)</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center; margin-top: 0.1rem; font-size: 0.9em;'>현재 시간: {current_time_str}</p>", unsafe_allow_html=True)
    st.markdown("---", unsafe_allow_html=True)

    all_available_dates_desc = get_all_available_sheet_dates_from_excel_drive(current_drive_service, SM_FILE_ID, "SM재고현황.xlsx")
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
        df_sm_trend_raw = load_sm_data_from_excel_drive(current_drive_service, SM_FILE_ID, report_date_str_list_yyyymmdd, "SM재고현황.xlsx")
    
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
                st.error(f"재고량 추이 차트 생성 오류: {e_chart1}") 
        elif dates_for_report: 
            st.write("표시할 그래프 데이터가 없습니다.")
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

            combined_table = pd.DataFrame(index=table_pivot_qty.index, columns=pd.MultiIndex.from_tuples([(ts, KOREAN_DAYS[ts.weekday()]) for ts in table_pivot_qty.columns], names=['날짜', '요일_temp']), dtype=object)
            
            for date_col_ts in table_pivot_qty.columns: 
                qty_series = table_pivot_qty[date_col_ts]; wgt_series = table_pivot_wgt[date_col_ts]; diff_series = qty_diff.get(date_col_ts)
                cell_strings = []
                for warehouse in table_pivot_qty.index: # combined_table.index 대신 table_pivot_qty.index 사용
                    qty_val = qty_series.get(warehouse, 0); wgt_val = wgt_series.get(warehouse, 0); diff_val = diff_series.get(warehouse, None) if diff_series is not None else None
                    base_string = f"{qty_val:,.0f} / {wgt_val:,.1f} Kg"; indicator = ""
                    if qty_val == 0 and wgt_val == 0: cell_strings.append("-") 
                    else:
                        if pd.notnull(diff_val) and len(table_pivot_qty.columns) > 1: 
                            if diff_val > 0.01: indicator = "🔺 " 
                            elif diff_val < -0.01: indicator = "▼ "
                        cell_strings.append(f"{indicator}{base_string}")
                combined_table[(date_col_ts, KOREAN_DAYS[date_col_ts.weekday()])] = cell_strings # MultiIndex 컬럼에 할당
            
            total_row_data = {}
            for date_col_ts in table_pivot_qty.columns:
                total_qty_val = daily_qty_totals.get(date_col_ts, 0); total_wgt_val = daily_wgt_totals.get(date_col_ts, 0); total_diff_val = total_qty_diff.get(date_col_ts, None)
                base_total_string = f"{total_qty_val:,.0f} / {total_wgt_val:,.1f} Kg"; total_indicator = ""
                if total_qty_val == 0 and total_wgt_val == 0: 
                    total_row_data[(date_col_ts, KOREAN_DAYS[date_col_ts.weekday()])] = "-"
                else:
                    if pd.notnull(total_diff_val) and len(daily_qty_totals.index) > 1: 
                        if total_diff_val > 0.01: total_indicator = "🔺 "
                        elif total_diff_val < -0.01: total_indicator = "▼ "
                    total_row_data[(date_col_ts, KOREAN_DAYS[date_col_ts.weekday()])] = f"{total_indicator}{base_total_string}"
            
            combined_table.loc['합계'] = pd.Series(total_row_data)
            combined_table.columns = [f"{ts.strftime('%m/%d')}({day})" for ts, day in combined_table.columns] # 최종 컬럼명 포맷팅
            
            combined_table_display = combined_table.reindex(REPORT_ROW_ORDER_TREND + ['합계'])
            st.dataframe(combined_table_display.reset_index().rename(columns={'index': '창고명'}), hide_index=True, use_container_width=True, height=300) 
        except Exception as e_table: 
            st.error(f"표 데이터 생성 중 오류: {e_table}")
            traceback.print_exc()
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

    latest_purchase_date = get_latest_date_from_log_drive(current_drive_service, PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, "입고내역.xlsx")
    latest_sales_date = get_latest_date_from_log_drive(current_drive_service, SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, "출고내역.xlsx")

    overall_latest_date = None 
    if latest_purchase_date and latest_sales_date: overall_latest_date = max(latest_purchase_date, latest_sales_date)
    elif latest_purchase_date: overall_latest_date = latest_purchase_date
    elif latest_sales_date: overall_latest_date = latest_sales_date
    
    if overall_latest_date:
        end_date_7day = overall_latest_date 
        start_date_7day = end_date_7day - datetime.timedelta(days=6) 
        period_caption = f"기간: {start_date_7day.strftime('%Y-%m-%d')} ~ {end_date_7day.strftime('%Y-%m-%d')}"
        actual_7day_date_range = [start_date_7day + datetime.timedelta(days=i) for i in range(7)] 

        log_cols = st.columns(2)
        with log_cols[0]:
            st.markdown("<h4 style='font-size:1.0rem; margin-bottom:0.1rem;'>일별 입고 현황 (Box/Kg)</h4>", unsafe_allow_html=True)
            st.caption(period_caption)
            df_purchase_daily_raw = load_daily_log_data_for_period_from_excel_drive(
                current_drive_service, PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME,
                PURCHASE_DATE_COL, PURCHASE_LOCATION_COL, PURCHASE_QTY_BOX_COL, PURCHASE_QTY_KG_COL,
                start_date_7day, end_date_7day,
                is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx"
            )

            if df_purchase_daily_raw is not None and not df_purchase_daily_raw.empty:
                purchase_pivot_box = df_purchase_daily_raw.pivot_table(index=PURCHASE_LOCATION_COL, columns='날짜', values='TotalQtyBox', fill_value=0)
                purchase_pivot_kg = df_purchase_daily_raw.pivot_table(index=PURCHASE_LOCATION_COL, columns='날짜', values='TotalQtyKg', fill_value=0)
                purchase_pivot_box = purchase_pivot_box.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)
                purchase_pivot_kg = purchase_pivot_kg.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)
                
                purchase_combined_table = pd.DataFrame(index=purchase_pivot_box.index, columns=pd.MultiIndex.from_tuples([(d, KOREAN_DAYS[d.weekday()]) for d in purchase_pivot_box.columns]), dtype=object)
                daily_purchase_totals_box = purchase_pivot_box.sum(axis=0) 
                daily_purchase_totals_kg = purchase_pivot_kg.sum(axis=0)  

                for date_col_obj in purchase_pivot_box.columns: 
                    for loc_idx, loc in enumerate(purchase_combined_table.index): # index로 접근
                        box = purchase_pivot_box.iloc[loc_idx][date_col_obj]
                        kg = purchase_pivot_kg.iloc[loc_idx][date_col_obj]
                        purchase_combined_table.loc[loc, (date_col_obj, KOREAN_DAYS[date_col_obj.weekday()])] = f"{box:,.0f} / {kg:,.1f}" if not (box == 0 and kg == 0) else "-"
                
                total_row_data_p = {}
                for date_obj in purchase_pivot_box.columns:
                    total_val_str = f"{daily_purchase_totals_box.get(date_obj, 0):,.0f} / {daily_purchase_totals_kg.get(date_obj, 0):,.1f}"
                    total_row_data_p[(date_obj, KOREAN_DAYS[date_obj.weekday()])] = total_val_str if not (daily_purchase_totals_box.get(date_obj, 0) == 0 and daily_purchase_totals_kg.get(date_obj, 0) == 0) else "-"
                
                purchase_combined_table.loc['합계'] = pd.Series(total_row_data_p)
                purchase_combined_table.columns = [f"{d.strftime('%m/%d')}({day})" for d, day in purchase_combined_table.columns]
                st.dataframe(purchase_combined_table.reset_index().rename(columns={'index': '지점명'}), hide_index=True, use_container_width=True, height=250) 
            else:
                st.write("해당 기간 입고 데이터가 없습니다.")
        
        with log_cols[1]:
            st.markdown("<h4 style='font-size:1.0rem; margin-bottom:0.1rem;'>일별 출고 현황 (Box/Kg)</h4>", unsafe_allow_html=True)
            st.caption(period_caption)
            df_sales_daily_raw = load_daily_log_data_for_period_from_excel_drive(
                current_drive_service, SALES_FILE_ID, SALES_LOG_SHEET_NAME,
                SALES_DATE_COL, SALES_LOCATION_COL, SALES_QTY_BOX_COL, SALES_QTY_KG_COL,
                start_date_7day, end_date_7day,
                file_name_for_error_msg="출고내역.xlsx"
            )

            if df_sales_daily_raw is not None and not df_sales_daily_raw.empty:
                sales_pivot_box = df_sales_daily_raw.pivot_table(index=SALES_LOCATION_COL, columns='날짜', values='TotalQtyBox', fill_value=0)
                sales_pivot_kg = df_sales_daily_raw.pivot_table(index=SALES_LOCATION_COL, columns='날짜', values='TotalQtyKg', fill_value=0)
                sales_pivot_box = sales_pivot_box.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)
                sales_pivot_kg = sales_pivot_kg.reindex(index=SUMMARY_TABLE_LOCATIONS, columns=actual_7day_date_range, fill_value=0)

                sales_combined_table = pd.DataFrame(index=sales_pivot_box.index, columns=pd.MultiIndex.from_tuples([(d, KOREAN_DAYS[d.weekday()]) for d in sales_pivot_box.columns]), dtype=object)
                daily_sales_totals_box = sales_pivot_box.sum(axis=0)
                daily_sales_totals_kg = sales_pivot_kg.sum(axis=0)

                for date_col_obj in sales_pivot_box.columns: 
                    for loc_idx, loc in enumerate(sales_combined_table.index): # index로 접근
                        box = sales_pivot_box.iloc[loc_idx][date_col_obj]
                        kg = sales_pivot_kg.iloc[loc_idx][date_col_obj]
                        sales_combined_table.loc[loc, (date_col_obj, KOREAN_DAYS[date_col_obj.weekday()])] = f"{box:,.0f} / {kg:,.1f}" if not (box == 0 and kg == 0) else "-"
                
                total_row_data_s = {}
                for date_obj in sales_pivot_box.columns:
                    total_val_str = f"{daily_sales_totals_box.get(date_obj, 0):,.0f} / {daily_sales_totals_kg.get(date_obj, 0):,.1f}"
                    total_row_data_s[(date_obj, KOREAN_DAYS[date_obj.weekday()])] = total_val_str if not (daily_sales_totals_box.get(date_obj, 0) == 0 and daily_sales_totals_kg.get(date_obj, 0) == 0) else "-"
                
                sales_combined_table.loc['합계'] = pd.Series(total_row_data_s)
                sales_combined_table.columns = [f"{d.strftime('%m/%d')}({day})" for d, day in sales_combined_table.columns]
                st.dataframe(sales_combined_table.reset_index().rename(columns={'index': '지점명'}), hide_index=True, use_container_width=True, height=250) 
            else:
                st.write("해당 기간 출고 데이터가 없습니다.")
    else:
        st.write("입고/출고 데이터를 가져올 수 없습니다 (최신 날짜 정보 없음).")

    st.markdown("---") 

    # --- 네 번째 행 (칸 6): 전년 동기 중량 비교 (가로 전체) ---
    st.markdown(f"{title_style}6. 전년 동기 중량 비교 (Kg)</h3>", unsafe_allow_html=True)
    today = datetime.date.today()
    current_year_start = today.replace(month=1, day=1); current_year_end = today
    previous_year_start = current_year_start - relativedelta(years=1); previous_year_end = current_year_end - relativedelta(years=1)
    st.caption(f"기간: 올해({current_year_start.strftime('%y/%m/%d')}~{current_year_end.strftime('%y/%m/%d')}) vs 작년({previous_year_start.strftime('%y/%m/%d')}~{previous_year_end.strftime('%y/%m/%d')})")

    df_sales_cy = load_log_data_for_period_from_excel_drive(current_drive_service, SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL, current_year_start, current_year_end, file_name_for_error_msg="출고내역.xlsx")
    df_sales_py = load_log_data_for_period_from_excel_drive(current_drive_service, SALES_FILE_ID, SALES_LOG_SHEET_NAME, SALES_DATE_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL, previous_year_start, previous_year_end, file_name_for_error_msg="출고내역.xlsx")
    df_purchase_cy = load_log_data_for_period_from_excel_drive(current_drive_service, PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, PURCHASE_QTY_KG_COL, PURCHASE_LOCATION_COL, current_year_start, current_year_end, is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx")
    df_purchase_py = load_log_data_for_period_from_excel_drive(current_drive_service, PURCHASE_FILE_ID, PURCHASE_LOG_SHEET_NAME, PURCHASE_DATE_COL, PURCHASE_QTY_KG_COL, PURCHASE_LOCATION_COL, previous_year_start, previous_year_end, is_purchase_log=True, file_name_for_error_msg="입고내역.xlsx")
    
    def prepare_comparison_df(df_cy, df_py, name_prefix):
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

    def plot_comparison_chart(df_combined, title):
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
        df_purchase_compare = prepare_comparison_df(df_purchase_cy, df_purchase_py, "입고")
        plot_comparison_chart(df_purchase_compare, "월별 입고 중량 비교")
    
    with comparison_cols[1]: 
        df_sales_compare = prepare_comparison_df(df_sales_cy, df_sales_py, "출고")
        plot_comparison_chart(df_sales_compare, "월별 출고 중량 비교")

# --- 앱 실행 부분 ---
if __name__ == "__main__":
    current_drive_service_on_load = st.session_state.get('drive_service')
    if current_drive_service_on_load is not None:
        render_daily_trend_page_layout()
    else:
        st.error("Google Drive 인증 정보를 로드하지 못했습니다. 앱 설정을 확인하거나 앱을 재시작해주세요. (메인 실행 블록)")
        if not IS_CLOUD_ENVIRONMENT:
            st.info(f"로컬 실행 중이라면, 코드 내의 SERVICE_ACCOUNT_FILE_PATH ('{SERVICE_ACCOUNT_FILE_PATH}')가 올바른지 확인해주세요.")

