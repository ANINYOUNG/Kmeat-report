# pages/4_재고_보충_제안.py fix-20250613

import streamlit as st
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import io
import traceback
import plotly.express as px # 그래프 생성을 위해 plotly 추가

# common_utils.py 에서 공통 유틸리티 함수 가져오기
from common_utils import download_excel_from_drive_as_bytes, get_all_available_sheet_dates_from_bytes

# --- Google Drive 파일 ID 정의 ---
SALES_FILE_ID = "1h-V7kIoInXgGLll7YBW5V_uZdF3Q1PdY"  # 매출내역 파일 ID
SM_FILE_ID = "1tRljdvOpp4fITaVEXvoL9mNveNg2qt4p"    # SM재고현황 파일 ID
# --- 파일 ID 정의 끝 ---

# --- 이 페이지 고유의 설정 ---
SALES_DATA_SHEET_NAME = 's-list' # 매출내역 파일의 시트 이름

# 컬럼명 상수
SALES_DATE_COL = '매출일자'
SALES_PROD_CODE_COL = '상품코드'
SALES_PROD_NAME_COL = '상  품  명' # 원본 엑셀의 컬럼명에 맞춤 (공백 주의)
SALES_QTY_BOX_COL = '수량(Box)'
SALES_QTY_KG_COL = '수량(Kg)'
SALES_LOCATION_COL = '지점명'

CURRENT_STOCK_PROD_CODE_COL = '상품코드'
CURRENT_STOCK_PROD_NAME_COL = '상품명'
CURRENT_STOCK_QTY_COL = '잔량(박스)'
CURRENT_STOCK_WGT_COL = '잔량(Kg)'
CURRENT_STOCK_LOCATION_COL = '지점명'

# --- Google Drive 서비스 객체 가져오기 ---
retrieved_drive_service = st.session_state.get('drive_service')
page_title_for_debug = "재고 보충 제안 페이지"

if retrieved_drive_service:
    st.sidebar.info(f"'{page_title_for_debug}'에서 Drive Service 로드 성공!")
else:
    st.sidebar.error(f"'{page_title_for_debug}'에서 Drive Service 로드 실패! (None). 메인 페이지를 먼저 방문하여 인증을 완료해주세요.")

drive_service = retrieved_drive_service

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_sales_history_and_filter_3m(_drive_service, file_id_sales, sheet_name, num_months=3):
    """
    지정된 Google Drive 파일/시트에서 전체 매출 데이터를 로드하고,
    매출 데이터의 가장 마지막 날짜를 기준으로 이전 90일 데이터를 필터링하여
    [상품코드, 상품명, 지점명]별 총 출고량 및 매출 발생일 수를 반환합니다.
    num_months 파라미터는 월평균 계산의 기준이 됩니다.
    """
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (매출 데이터 로딩)")
        return pd.DataFrame()

    file_bytes_sales = download_excel_from_drive_as_bytes(_drive_service, file_id_sales, f"매출내역 ({sheet_name})")
    if file_bytes_sales is None:
        return pd.DataFrame()

    try:
        required_cols = [SALES_DATE_COL, SALES_PROD_CODE_COL, SALES_PROD_NAME_COL,
                         SALES_QTY_BOX_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL]

        df = pd.read_excel(file_bytes_sales, sheet_name=sheet_name)

        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            st.error(f"오류: 매출 내역 시트 '{sheet_name}' (ID: {file_id_sales})에 필요한 컬럼({missing_cols}) 없음")
            st.write(f"사용 가능한 컬럼: {df.columns.tolist()}")
            return pd.DataFrame()

        df[SALES_DATE_COL] = pd.to_datetime(df[SALES_DATE_COL], errors='coerce')
        df.dropna(subset=[SALES_DATE_COL], inplace=True)

        if df.empty:
            st.warning(f"매출내역 파일 (ID: {file_id_sales}, 시트: {sheet_name})에 유효한 날짜 데이터가 없습니다.")
            return pd.DataFrame()

        df[SALES_PROD_CODE_COL] = df[SALES_PROD_CODE_COL].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df[SALES_PROD_NAME_COL] = df[SALES_PROD_NAME_COL].astype(str).str.strip()
        df[SALES_LOCATION_COL] = df[SALES_LOCATION_COL].astype(str).str.strip()
        df[SALES_QTY_BOX_COL] = pd.to_numeric(df[SALES_QTY_BOX_COL], errors='coerce').fillna(0)
        df[SALES_QTY_KG_COL] = pd.to_numeric(df[SALES_QTY_KG_COL], errors='coerce').fillna(0)

        max_sales_date = df[SALES_DATE_COL].max()
        if pd.isna(max_sales_date):
            st.warning(f"매출 데이터 (ID: {file_id_sales}, 시트: {sheet_name})에서 유효한 최대 매출일자를 찾을 수 없습니다.")
            return pd.DataFrame()

        end_date_of_analysis_period = max_sales_date
        start_date_of_analysis_period = end_date_of_analysis_period - pd.Timedelta(days=89)

        st.info(f"매출 분석 기간 (데이터 마지막 날짜 기준 90일): {start_date_of_analysis_period.strftime('%Y-%m-%d')} ~ {end_date_of_analysis_period.strftime('%Y-%m-%d')}")

        df_filtered = df[
            (df[SALES_DATE_COL] >= start_date_of_analysis_period) &
            (df[SALES_DATE_COL] <= end_date_of_analysis_period)
        ].copy()

        if df_filtered.empty:
            st.warning(f"선택된 기간 ({start_date_of_analysis_period.strftime('%Y-%m-%d')} ~ {end_date_of_analysis_period.strftime('%Y-%m-%d')})의 매출 데이터가 '{sheet_name}' 시트에 없습니다.")
            return pd.DataFrame()

        total_sales_by_item_loc = df_filtered.groupby(
            [SALES_PROD_CODE_COL, SALES_PROD_NAME_COL, SALES_LOCATION_COL],
            as_index=False
        ).agg(
            TotalQtyBox=(SALES_QTY_BOX_COL, 'sum'),
            TotalQtyKg=(SALES_QTY_KG_COL, 'sum'),
            SalesDays=(SALES_DATE_COL, 'nunique')
        )
        return total_sales_by_item_loc
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve):
            st.error(f"오류: 매출 파일 (ID: {file_id_sales})에 '{sheet_name}' 시트 없음")
        else:
            st.error(f"매출 데이터 (ID: {file_id_sales}, 시트: {sheet_name}) 로드 중 값 오류: {ve}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"매출 데이터 (ID: {file_id_sales}, 시트: {sheet_name}) 로드/처리 중 예상 못한 오류: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_current_stock_data(_drive_service, file_id_sm):
    """SM재고현황 파일의 최신 날짜 시트에서 현재고 데이터를 로드합니다."""
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (현재고 데이터 로딩)")
        return pd.DataFrame()

    sm_file_bytes = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, "SM재고현황 (현재고 조회용)")
    if not sm_file_bytes:
        return pd.DataFrame()

    available_sm_dates = get_all_available_sheet_dates_from_bytes(sm_file_bytes, "SM재고현황 (현재고 조회용)")
    if not available_sm_dates:
        st.warning(f"SM재고현황 파일 (ID: {file_id_sm})에서 사용 가능한 재고 데이터 시트를 찾을 수 없습니다.")
        return pd.DataFrame()

    latest_date_obj = available_sm_dates[0]
    latest_date_str = latest_date_obj.strftime("%Y%m%d")
    st.info(f"현재고 기준일: {latest_date_obj.strftime('%Y-%m-%d')} (시트: {latest_date_str})")

    try:
        sm_file_bytes.seek(0)
        df_stock_raw = pd.read_excel(sm_file_bytes, sheet_name=latest_date_str)

        required_stock_cols = [CURRENT_STOCK_PROD_CODE_COL, CURRENT_STOCK_PROD_NAME_COL,
                               CURRENT_STOCK_QTY_COL, CURRENT_STOCK_WGT_COL, CURRENT_STOCK_LOCATION_COL]
<<<<<<< HEAD
=======
        df_stock_raw.rename(columns={'상 품 명': '상품명'}, inplace=True)
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3

        if not all(col in df_stock_raw.columns for col in required_stock_cols):
            missing = [col for col in required_stock_cols if col not in df_stock_raw.columns]
            st.error(f"현재고 데이터 시트('{latest_date_str}', ID: {file_id_sm})에 필수 컬럼이 없습니다: {missing}.")
            st.error("코드 상단의 현재고 관련 상수(CURRENT_STOCK_..._COL)와 실제 엑셀 파일의 컬럼명을 확인해주세요.")
            return pd.DataFrame()

        df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] = df_stock_raw[CURRENT_STOCK_PROD_CODE_COL].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_stock_raw[CURRENT_STOCK_PROD_NAME_COL] = df_stock_raw[CURRENT_STOCK_PROD_NAME_COL].astype(str).str.strip()
        df_stock_raw[CURRENT_STOCK_LOCATION_COL] = df_stock_raw[CURRENT_STOCK_LOCATION_COL].astype(str).str.strip()
        df_stock_raw[CURRENT_STOCK_QTY_COL] = pd.to_numeric(df_stock_raw[CURRENT_STOCK_QTY_COL], errors='coerce').fillna(0)
        df_stock_raw[CURRENT_STOCK_WGT_COL] = pd.to_numeric(df_stock_raw[CURRENT_STOCK_WGT_COL], errors='coerce').fillna(0)

        current_stock_by_item_loc = df_stock_raw.groupby(
            [CURRENT_STOCK_PROD_CODE_COL, CURRENT_STOCK_PROD_NAME_COL, CURRENT_STOCK_LOCATION_COL],
            as_index=False
        ).agg(
            CurrentQty=(CURRENT_STOCK_QTY_COL, 'sum'),
            CurrentWgt=(CURRENT_STOCK_WGT_COL, 'sum')
        )

        if current_stock_by_item_loc.empty and not df_stock_raw.empty:
            st.warning(f"현재고 데이터 그룹핑 후 데이터가 없습니다 (시트: {latest_date_str}, ID: {file_id_sm}). 컬럼명 또는 데이터 내용을 확인해주세요.")
            return pd.DataFrame()
        return current_stock_by_item_loc
    except ValueError as ve:
        if latest_date_str and f"Worksheet named '{latest_date_str}' not found" in str(ve):
            st.error(f"오류: 현재고 파일 (ID: {file_id_sm})에 '{latest_date_str}' 시트 없음")
        else:
            st.error(f"현재고 데이터 (ID: {file_id_sm}, 시트: {latest_date_str}) 로드 중 값 오류: {ve}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"현재고 데이터 (ID: {file_id_sm}, 시트: {latest_date_str}) 로드/처리 중 예외 발생: {e}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

<<<<<<< HEAD
# --- (기능 추가) 특정 품목의 재고 변동 내역을 가져오는 함수 ---
@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def get_stock_history_for_item(_drive_service, file_id_sm, search_term):
    """SM재고현황 파일의 모든 시트를 읽어 특정 상품의 90일간 재고 추이를 반환합니다."""
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다.")
        return pd.DataFrame(), None, None

    sm_file_bytes = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, "SM재고현황 (재고 추이 조회용)")
    if not sm_file_bytes:
        st.error("SM재고현황 파일을 다운로드할 수 없습니다.")
        return pd.DataFrame(), None, None

    try:
        xls = pd.ExcelFile(sm_file_bytes)
        all_sheets = xls.sheet_names
        sheet_dates = []
        for sheet in all_sheets:
            try:
                sheet_dates.append(datetime.datetime.strptime(sheet, "%Y%m%d"))
            except ValueError:
                continue

        sheet_dates.sort(reverse=True)
        today = datetime.datetime.now().date()
        ninety_days_ago = today - datetime.timedelta(days=90)
        
        relevant_sheets = [dt.strftime("%Y%m%d") for dt in sheet_dates if ninety_days_ago <= dt.date() <= today]

        if not relevant_sheets:
            st.warning("지난 90일간의 재고 데이터 시트가 없습니다.")
            return pd.DataFrame(), None, None
    except Exception as e:
        st.error(f"SM재고현황 파일의 시트를 읽는 중 오류 발생: {e}")
        return pd.DataFrame(), None, None

    is_code_search = search_term.isdigit()
    history = []
    found_product_name = ""
    found_product_code = ""

    for sheet_name in relevant_sheets:
        try:
            df_stock_raw = pd.read_excel(xls, sheet_name=sheet_name)

            df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] = df_stock_raw[CURRENT_STOCK_PROD_CODE_COL].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            df_stock_raw[CURRENT_STOCK_PROD_NAME_COL] = df_stock_raw[CURRENT_STOCK_PROD_NAME_COL].astype(str).str.strip()
            df_stock_raw[CURRENT_STOCK_QTY_COL] = pd.to_numeric(df_stock_raw[CURRENT_STOCK_QTY_COL], errors='coerce').fillna(0)

            if is_code_search:
                filtered_df = df_stock_raw[df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] == search_term].copy()
            else:
                filtered_df = df_stock_raw[df_stock_raw[CURRENT_STOCK_PROD_NAME_COL].str.contains(search_term, na=False)].copy()

            total_stock = filtered_df[CURRENT_STOCK_QTY_COL].sum()
            sheet_date = datetime.datetime.strptime(sheet_name, "%Y%m%d").date()
            history.append({'일자': sheet_date, '재고량(박스)': total_stock})
            
            if not found_product_name and not filtered_df.empty:
                 # 검색 결과의 첫번째 항목으로 상품정보를 설정
                found_product_name = filtered_df.iloc[0][CURRENT_STOCK_PROD_NAME_COL]
                found_product_code = filtered_df.iloc[0][CURRENT_STOCK_PROD_CODE_COL]

        except Exception:
            continue # 특정 시트 읽기 실패 시 건너뛰기

    if not history:
        st.warning(f"'{search_term}'에 대한 지난 90일간의 재고 기록을 찾을 수 없습니다.")
        return pd.DataFrame(), None, None

    history_df = pd.DataFrame(history).sort_values(by='일자').reset_index(drop=True)
    return history_df, found_product_code, found_product_name
# --- 함수 추가 끝 ---
=======
# --- 재고 추이 분석을 위한 함수들 ---

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def find_matching_products(_drive_service, file_id_sm, search_term):
    """가장 최신 재고 시트에서 검색어와 일치하는 모든 품목 리스트를 찾습니다."""
    if _drive_service is None: return []
    sm_file_bytes = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, "SM재고현황 (품목 검색용)")
    if not sm_file_bytes: return []

    available_sm_dates = get_all_available_sheet_dates_from_bytes(sm_file_bytes, "SM재고현황 (품목 검색용)")
    if not available_sm_dates: return []
    
    latest_date_str = available_sm_dates[0].strftime("%Y%m%d")
    try:
        sm_file_bytes.seek(0)
        df = pd.read_excel(sm_file_bytes, sheet_name=latest_date_str)
        df.rename(columns={'상 품 명': '상품명'}, inplace=True) # 컬럼명 오타 대응

        df[CURRENT_STOCK_PROD_CODE_COL] = df[CURRENT_STOCK_PROD_CODE_COL].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df[CURRENT_STOCK_PROD_NAME_COL] = df[CURRENT_STOCK_PROD_NAME_COL].astype(str).str.strip()
        
        if search_term.isdigit():
            matches = df[df[CURRENT_STOCK_PROD_CODE_COL] == search_term]
        else:
            matches = df[df[CURRENT_STOCK_PROD_NAME_COL].str.contains(search_term, na=False)]
        
        if matches.empty:
            return []
            
        unique_products = matches[[CURRENT_STOCK_PROD_CODE_COL, CURRENT_STOCK_PROD_NAME_COL]].drop_duplicates().values.tolist()
        return [tuple(prod) for prod in unique_products]
    except Exception:
        return []

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def get_stock_history_for_item_by_code(_drive_service, file_id_sm, product_code):
    """특정 '상품코드'를 기준으로 90일간의 재고 추이를 가져옵니다."""
    if not _drive_service or not product_code:
        return pd.DataFrame()

    sm_file_bytes = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, "SM재고현황 (재고 추이 조회용)")
    if not sm_file_bytes: return pd.DataFrame()
    
    try:
        xls = pd.ExcelFile(sm_file_bytes)
        all_sheets = xls.sheet_names
        sheet_dates = sorted([datetime.datetime.strptime(s, "%Y%m%d") for s in all_sheets if s.isdigit()], reverse=True)
        
        today = datetime.datetime.now().date()
        ninety_days_ago = today - datetime.timedelta(days=90)
        relevant_sheets = [dt.strftime("%Y%m%d") for dt in sheet_dates if ninety_days_ago <= dt.date() <= today]

        history = []
        for sheet_name in relevant_sheets:
            df_stock_raw = pd.read_excel(xls, sheet_name=sheet_name)
            df_stock_raw.rename(columns={'상 품 명': '상품명'}, inplace=True)
            df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] = df_stock_raw[CURRENT_STOCK_PROD_CODE_COL].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            df_stock_raw[CURRENT_STOCK_QTY_COL] = pd.to_numeric(df_stock_raw[CURRENT_STOCK_QTY_COL], errors='coerce').fillna(0)
            
            filtered_df = df_stock_raw[df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] == product_code]
            total_stock = filtered_df[CURRENT_STOCK_QTY_COL].sum()
            sheet_date = datetime.datetime.strptime(sheet_name, "%Y%m%d").date()
            history.append({'일자': sheet_date, '재고량(박스)': total_stock})

        if not history: return pd.DataFrame()
        return pd.DataFrame(history).sort_values(by='일자').reset_index(drop=True)
    except Exception:
        return pd.DataFrame()
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3


# --- Streamlit 페이지 UI 및 로직 ---
st.title("📦 재고 보충 제안 보고서 (지점별)")

if drive_service is None:
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()

MIN_SALES_DAYS_PER_MONTH = 5
st.markdown(f"""
최근 90일간의 데이터를 기반으로 월평균 출고량과 현재고를 **지점별로** 비교하여 보충 필요 수량을 제안합니다.
(여기서 '월평균'은 90일간 총 출고량을 3으로 나누어 계산합니다.)
**단, (90일 기준) 월평균 출고일수가 {MIN_SALES_DAYS_PER_MONTH}일 이상이고, 계산된 필요수량(박스)이 0보다 큰 품목만 대상**으로 합니다.
""")

st.markdown(f"매출 데이터 원본: Google Drive 파일 (ID: `{SALES_FILE_ID}`)의 '{SALES_DATA_SHEET_NAME}' 시트")
st.markdown(f"현재고 데이터 원본: Google Drive 파일 (ID: `{SM_FILE_ID}`)의 최신 날짜 시트")
st.markdown("---")

<<<<<<< HEAD
# --- 기존 재고 보충 제안 로직 (수정 없음) ---
# (이 부분은 기존 코드와 동일합니다)
=======
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
num_months_to_analyze = 3
df_total_sales_90d = load_sales_history_and_filter_3m(drive_service, SALES_FILE_ID, SALES_DATA_SHEET_NAME, num_months=num_months_to_analyze)
df_current_stock = load_current_stock_data(drive_service, SM_FILE_ID)

if df_total_sales_90d.empty or df_current_stock.empty:
    st.warning("매출 데이터 또는 현재고 데이터가 없어 보고서를 생성할 수 없습니다. 위의 로그 메시지를 확인해주세요.")
else:
    df_avg_monthly_sales = df_total_sales_90d.copy()
    df_avg_monthly_sales['월평균 출고량(박스)'] = (df_avg_monthly_sales['TotalQtyBox'] / num_months_to_analyze).round(2)
    df_avg_monthly_sales['월평균 출고량(Kg)'] = (df_avg_monthly_sales['TotalQtyKg'] / num_months_to_analyze).round(2)
    df_avg_monthly_sales['월평균 출고일수'] = (df_avg_monthly_sales['SalesDays'] / num_months_to_analyze).round(2)

    df_avg_monthly_sales_filtered = df_avg_monthly_sales[df_avg_monthly_sales['월평균 출고일수'] >= MIN_SALES_DAYS_PER_MONTH].copy()

    if df_avg_monthly_sales_filtered.empty:
        st.warning(f"계산된 월평균 출고일수가 {MIN_SALES_DAYS_PER_MONTH}일 이상인 품목이 없습니다. 보고서를 생성할 수 없습니다.")
    else:
        st.success(f"총 {len(df_avg_monthly_sales)}개 품목(지점별, 90일 기준) 중 계산된 월평균 출고일수 {MIN_SALES_DAYS_PER_MONTH}일 이상인 {len(df_avg_monthly_sales_filtered)}개 품목을 대상으로 분석합니다.")
    
        df_avg_monthly_sales_to_use = df_avg_monthly_sales_filtered.rename(columns={
<<<<<<< HEAD
            SALES_PROD_CODE_COL: '상품코드',
=======
            SALES_PROD_CODE_COL: '상품코드', 
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
            SALES_PROD_NAME_COL: '상품명',
            SALES_LOCATION_COL: '지점명'
        })
        df_avg_monthly_sales_to_use['상품코드'] = df_avg_monthly_sales_to_use['상품코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_avg_monthly_sales_to_use['지점명'] = df_avg_monthly_sales_to_use['지점명'].astype(str).str.strip()
        df_avg_monthly_sales_to_use = df_avg_monthly_sales_to_use[['상품코드', '상품명', '지점명', '월평균 출고량(박스)', '월평균 출고량(Kg)', '월평균 출고일수']]

        df_current_stock_report = df_current_stock.rename(columns={
            CURRENT_STOCK_PROD_CODE_COL: '상품코드',
<<<<<<< HEAD
            CURRENT_STOCK_PROD_NAME_COL: '상품명',
=======
            CURRENT_STOCK_PROD_NAME_COL: '상품명', 
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
            CURRENT_STOCK_LOCATION_COL: '지점명',
            'CurrentQty': '잔량(박스)',
            'CurrentWgt': '잔량(Kg)'
        })
        df_current_stock_report['상품코드'] = df_current_stock_report['상품코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df_current_stock_report['지점명'] = df_current_stock_report['지점명'].astype(str).str.strip()
        df_current_stock_report = df_current_stock_report[['상품코드', '지점명', '상품명', '잔량(박스)', '잔량(Kg)']]

        df_report = pd.merge(
<<<<<<< HEAD
            df_avg_monthly_sales_to_use,
            df_current_stock_report,
            on=['상품코드', '지점명'],
            how='left',
            suffixes=('_sales', '_stock')
        )

        df_report['상품명'] = df_report['상품명_sales'].fillna(df_report['상품명_stock'])
        df_report.drop(columns=['상품명_sales', '상품명_stock'], inplace=True, errors='ignore')

=======
            df_avg_monthly_sales_to_use, 
            df_current_stock_report, 
            on=['상품코드', '지점명'], 
            how='left', 
            suffixes=('_sales', '_stock') 
        )
        
        df_report['상품명'] = df_report['상품명_sales'].fillna(df_report['상품명_stock'])
        df_report.drop(columns=['상품명_sales', '상품명_stock'], inplace=True, errors='ignore') 
        
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
        df_report['잔량(박스)'] = df_report['잔량(박스)'].fillna(0)
        df_report['잔량(Kg)'] = df_report['잔량(Kg)'].fillna(0)

        df_report['필요수량(박스)'] = (df_report['월평균 출고량(박스)'] - df_report['잔량(박스)']).apply(lambda x: max(0, x)).round(2)
        df_report['필요수량(Kg)'] = (df_report['월평균 출고량(Kg)'] - df_report['잔량(Kg)']).apply(lambda x: max(0, x)).round(2)
<<<<<<< HEAD

=======
        
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
        df_report_filtered_needed = df_report[df_report['필요수량(박스)'] > 0].copy()

        if df_report_filtered_needed.empty:
            st.info(f"계산된 월평균 출고일수 {MIN_SALES_DAYS_PER_MONTH}일 이상인 품목 중 현재 보충이 필요한 품목(필요수량(박스) > 0)은 없습니다.")
        else:
            final_report_columns = [
<<<<<<< HEAD
                '지점명', '상품코드', '상품명',
                '잔량(박스)', '잔량(Kg)',
                '월평균 출고량(박스)', '월평균 출고량(Kg)',
                '월평균 출고일수',
                '필요수량(박스)', '필요수량(Kg)'
            ]
            existing_final_cols = [col for col in final_report_columns if col in df_report_filtered_needed.columns]
            df_report_final = df_report_filtered_needed[existing_final_cols]
=======
                '지점명', '상품코드', '상품명', 
                '잔량(박스)', '잔량(Kg)', 
                '월평균 출고량(박스)', '월평균 출고량(Kg)',
                '월평균 출고일수', 
                '필요수량(박스)', '필요수량(Kg)'
            ]
            existing_final_cols = [col for col in final_report_columns if col in df_report_filtered_needed.columns] 
            df_report_final = df_report_filtered_needed[existing_final_cols]

>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
            df_report_final = df_report_final.sort_values(by=['지점명', '필요수량(박스)'], ascending=[True, False])

            st.markdown("---")
            st.header("📋 재고 보충 제안 리스트 (지점별)")
<<<<<<< HEAD

            df_display = df_report_final.copy()

            if '상품코드' in df_display.columns:
                try:
                    df_display['상품코드'] = df_display['상품코드'].astype(str).str.replace(r'\.0$', '', regex=True)
                except Exception as e_strip:
                    st.warning(f"상품코드 문자열 변환 중 경미한 오류: {e_strip}")
                    df_display['상품코드'] = df_display['상품코드'].astype(str)
=======
            
            df_display = df_report_final.copy() 

            if '상품코드' in df_display.columns:
                try: 
                    df_display['상품코드'] = df_display['상품코드'].astype(str).str.replace(r'\.0$', '', regex=True)
                except Exception as e_strip:
                    st.warning(f"상품코드 문자열 변환 중 경미한 오류: {e_strip}")
                    df_display['상품코드'] = df_display['상품코드'].astype(str) 
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3

            cols_to_make_int_for_display = ['월평균 출고량(박스)', '필요수량(박스)', '잔량(박스)']
            for col in cols_to_make_int_for_display:
                if col in df_display.columns:
                    df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0).round(0).astype('Int64')

            format_dict = {}
            for col in ['잔량(박스)', '월평균 출고량(박스)', '필요수량(박스)']:
<<<<<<< HEAD
                if col in df_display.columns:
                    format_dict[col] = "{:,.0f}"

            for col in ['잔량(Kg)', '월평균 출고량(Kg)', '필요수량(Kg)']:
                if col in df_display.columns:
                    format_dict[col] = "{:,.2f}"

            if '월평균 출고일수' in df_display.columns:
                format_dict['월평균 출고일수'] = "{:,.2f}"

=======
                if col in df_display.columns: 
                    format_dict[col] = "{:,.0f}" 
            
            for col in ['잔량(Kg)', '월평균 출고량(Kg)', '필요수량(Kg)']:
                if col in df_display.columns: 
                    format_dict[col] = "{:,.2f}" 
            
            if '월평균 출고일수' in df_display.columns: 
                format_dict['월평균 출고일수'] = "{:,.2f}"
                
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
            def highlight_refrigerated_product_name(val):
                if isinstance(val, str) and "냉장" in val:
                    return 'color: red'
                return ''

            st.dataframe(
                df_display.style.format(format_dict, na_rep="-")
                .map(highlight_refrigerated_product_name, subset=['상품명'])
<<<<<<< HEAD
                .set_properties(**{'text-align': 'right'}),
                use_container_width=True
            )

            @st.cache_data
            def convert_df_to_excel(df_to_convert):
                excel_stream = io.BytesIO()
                with pd.ExcelWriter(excel_stream, engine='xlsxwriter') as writer:
                    df_to_convert.to_excel(writer, index=False, sheet_name='보고서')
                excel_stream.seek(0)
=======
                .set_properties(**{'text-align': 'right'}), 
                use_container_width=True
            )

            @st.cache_data 
            def convert_df_to_excel(df_to_convert):
                excel_stream = io.BytesIO()
                with pd.ExcelWriter(excel_stream, engine='xlsxwriter') as writer: 
                    df_to_convert.to_excel(writer, index=False, sheet_name='보고서')
                excel_stream.seek(0) 
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
                return excel_stream.getvalue()

            if not df_display.empty:
                excel_data = convert_df_to_excel(df_display)
                report_date_str = datetime.date.today().strftime("%Y%m%d")
                st.download_button(
                    label="📥 보고서 엑셀로 다운로드",
                    data=excel_data,
                    file_name=f"재고보충제안보고서_지점별_{report_date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
<<<<<<< HEAD
                    key="download_replenishment_report_formatted_page_filtered_no_zero_needed_v3"
                )

# --- (기능 추가) 개별 품목 재고 추이 조회 UI ---
st.markdown("---")
st.header("🔍 개별 품목 재고 추이 조회")

search_term = st.text_input("조회할 상품의 상품코드 또는 상품명을 입력하세요:", key="stock_trace_search_input")

if st.button("재고 추이 조회", key="stock_trace_search_button"):
    if not search_term.strip():
        st.warning("상품코드 또는 상품명을 입력해주세요.")
    else:
        with st.spinner(f"'{search_term}'에 대한 재고 기록을 조회하는 중입니다..."):
            history_df, p_code, p_name = get_stock_history_for_item(drive_service, SM_FILE_ID, search_term.strip())

        if not history_df.empty:
            st.success(f"**{p_name} (코드: {p_code})** 재고 변동 내역")

            # 1. 1주일간의 일별 재고 변동 (표)
            st.subheader("🗓️ 최근 1주일 재고 변동")
            one_week_ago = datetime.datetime.now().date() - datetime.timedelta(days=7)
            history_df['일자_dt'] = pd.to_datetime(history_df['일자']).dt.date
            
            weekly_df = history_df[history_df['일자_dt'] >= one_week_ago].copy()
            weekly_df['일자'] = weekly_df['일자'].apply(lambda x: x.strftime('%Y-%m-%d (%a)'))
            
            st.dataframe(weekly_df[['일자', '재고량(박스)']].style.format({'재고량(박스)': "{:,.0f}"}), use_container_width=True)

            # 2. 3개월 동안의 재고 변동 (그래프)
            st.subheader("📈 최근 3개월 재고 변동 그래프")
            
            fig = px.line(history_df, x='일자', y='재고량(박스)', title=f'{p_name} 재고 변동 추이 (90일)', markers=True)
            fig.update_layout(
                xaxis_title='일자',
                yaxis_title='재고량(박스)',
                yaxis_tickformat=','
            )
            st.plotly_chart(fig, use_container_width=True)
# --- UI 추가 끝 ---
=======
                    key="download_replenishment_report_formatted_page_filtered_no_zero_needed_v3" 
                )

# --- 개별 품목 재고 추이 조회 UI ---
st.markdown("---")
st.header("🔍 개별 품목 재고 추이 조회")

# 세션 상태 초기화
if 'product_choices' not in st.session_state:
    st.session_state.product_choices = None
if 'selected_product' not in st.session_state:
    st.session_state.selected_product = None

search_term = st.text_input("조회할 상품의 상품코드 또는 상품명을 입력하세요:", key="stock_trace_search_input")

if st.button("품목 검색", key="stock_trace_search_button"):
    # 버튼을 누를 때마다 이전 선택 상태를 초기화
    st.session_state.product_choices = None
    st.session_state.selected_product = None
    if search_term.strip():
        with st.spinner("일치하는 품목을 찾고 있습니다..."):
            choices = find_matching_products(drive_service, SM_FILE_ID, search_term.strip())
            if choices:
                st.session_state.product_choices = choices
            else:
                st.warning("일치하는 품목이 없습니다.")
    else:
        st.warning("상품코드 또는 상품명을 입력해주세요.")

# 검색 결과가 세션에 저장되어 있을 경우 선택 UI를 표시
if st.session_state.product_choices:
    choices = st.session_state.product_choices
    if len(choices) == 1:
        # 검색 결과가 하나뿐이면 자동으로 선택
        st.session_state.selected_product = choices[0]
        # 사용자에게 자동 선택되었음을 알림
        st.info(f"유일한 품목 **{choices[0][1]}** 이(가) 자동 선택되었습니다.")
    else:
        # 검색 결과가 여러 개이면 사용자에게 선택지를 제공
        display_choices = ["아래 목록에서 하나를 선택하세요..."] + choices
        
        selected = st.selectbox(
            label="여러 품목이 검색되었습니다. 조회할 품목을 선택하세요.",
            options=display_choices,
            format_func=lambda x: x if isinstance(x, str) else f"{x[1]} ({x[0]})"
        )
        if isinstance(selected, tuple): # 사용자가 유효한 품목을 선택한 경우
            st.session_state.selected_product = selected
        else:
            st.session_state.selected_product = None # "선택하세요"를 고른 경우 선택 해제

# 최종 품목이 선택되었을 때만 재고 추이 분석을 실행
if st.session_state.selected_product:
    p_code, p_name = st.session_state.selected_product
    with st.spinner(f"**{p_name}**의 재고 기록을 조회하는 중입니다..."):
        # 상품명을 인자로 넘기지 않도록 수정
        history_df = get_stock_history_for_item_by_code(drive_service, SM_FILE_ID, p_code)

    if not history_df.empty:
        st.success(f"**{p_name} (코드: {p_code})** 재고 변동 내역")

        # 1. 1주일간의 일별 재고 변동 (표)
        st.subheader("🗓️ 최근 1주일 재고 변동")
        one_week_ago = datetime.datetime.now().date() - datetime.timedelta(days=7)
        history_df['일자_dt'] = pd.to_datetime(history_df['일자']).dt.date
        
        weekly_df = history_df[history_df['일자_dt'] > one_week_ago].copy()
        weekly_df['일자'] = weekly_df['일자'].apply(lambda x: x.strftime('%Y-%m-%d (%a)'))
        
        st.dataframe(
            weekly_df[['일자', '재고량(박스)']].set_index('일자').style.format({'재고량(박스)': "{:,.0f}"}),
            use_container_width=True
        )

        # 2. 3개월 동안의 재고 변동 (그래프)
        st.subheader("📈 최근 3개월 재고 변동 그래프")
        
        fig = px.line(history_df, x='일자', y='재고량(박스)', title=f'{p_name} 재고 변동 추이 (90일)', markers=True)
        fig.update_layout(
            xaxis_title='일자',
            yaxis_title='재고량(박스)',
            yaxis_tickformat=','
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error(f"**{p_name}**의 재고 내역을 조회하는 데 실패했거나 데이터가 없습니다.")
>>>>>>> 46ccaad40de1fda1a1b06e92242795ac69bdc0f3
