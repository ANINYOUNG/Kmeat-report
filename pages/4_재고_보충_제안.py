# pages/4_재고_보충_제안.py (ImportError 및 Cloud용 수정, 출고빈도 필터 추가)

import streamlit as st
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import io # BytesIO 사용을 위해 필요
import traceback # 예외 처리용

# common_utils.py 에서 공통 유틸리티 함수 가져오기
# DATA_FOLDER, SM_FILE은 로컬 경로이므로 common_utils에서 가져오지 않습니다.
# get_all_available_sheet_dates 대신 get_all_available_sheet_dates_from_bytes를 사용합니다.
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
SALES_PROD_NAME_COL = '상  품  명' 
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
    지난 N개의 완전한 달력 월 데이터를 필터링하여 
    [상품코드, 상품명, 지점명]별 총 출고량 및 매출 발생일 수를 반환합니다.
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
        
        df[SALES_PROD_CODE_COL] = df[SALES_PROD_CODE_COL].astype(str).str.strip()
        df[SALES_PROD_NAME_COL] = df[SALES_PROD_NAME_COL].astype(str).str.strip()
        df[SALES_LOCATION_COL] = df[SALES_LOCATION_COL].astype(str).str.strip()
        df[SALES_QTY_BOX_COL] = pd.to_numeric(df[SALES_QTY_BOX_COL], errors='coerce').fillna(0)
        df[SALES_QTY_KG_COL] = pd.to_numeric(df[SALES_QTY_KG_COL], errors='coerce').fillna(0)

        if df.empty:
            st.warning(f"매출내역 파일 (ID: {file_id_sales}, 시트: {sheet_name})에 유효한 데이터가 없습니다.")
            return pd.DataFrame()

        today = pd.Timestamp.today().normalize()
        first_day_of_current_month = today.replace(day=1)
        end_date_of_last_full_month = first_day_of_current_month - pd.Timedelta(days=1) 
        start_date_of_analysis_period = (end_date_of_last_full_month + pd.Timedelta(days=1) - pd.DateOffset(months=num_months)).replace(day=1)
        
        st.info(f"매출 분석 기간 (지난 {num_months}개월): {start_date_of_analysis_period.strftime('%Y-%m-%d')} ~ {end_date_of_last_full_month.strftime('%Y-%m-%d')}")

        df_filtered = df[
            (df[SALES_DATE_COL] >= start_date_of_analysis_period) &
            (df[SALES_DATE_COL] <= end_date_of_last_full_month)
        ].copy()

        if df_filtered.empty:
            st.warning(f"선택된 기간의 매출 데이터가 '{sheet_name}' 시트에 없습니다.")
            return pd.DataFrame()
        
        # [상품코드, 상품명, 지점명]별로 그룹화하여 총 출고량과 *매출 발생일 수* 집계
        total_sales_by_item_loc = df_filtered.groupby(
            [SALES_PROD_CODE_COL, SALES_PROD_NAME_COL, SALES_LOCATION_COL], 
            as_index=False
        ).agg(
            TotalQtyBox=(SALES_QTY_BOX_COL, 'sum'),
            TotalQtyKg=(SALES_QTY_KG_COL, 'sum'),
            SalesDays=(SALES_DATE_COL, 'nunique') # <<< 변경점: 매출 발생 고유 일자 수 추가
        )
        return total_sales_by_item_loc
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve):
            st.error(f"오류: 매출 파일 (ID: {file_id_sales})에 '{sheet_name}' 시트 없음")
        else:
            st.error(f"매출 데이터 (ID: {file_id_sales}) 로드 중 값 오류: {ve}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"매출 데이터 (ID: {file_id_sales}) 로드/처리 중 예상 못한 오류: {e}")
        st.error(traceback.format_exc()) # 상세 오류 로그 추가
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
        
        if not all(col in df_stock_raw.columns for col in required_stock_cols):
            missing = [col for col in required_stock_cols if col not in df_stock_raw.columns]
            st.error(f"현재고 데이터 시트({latest_date_str}, ID: {file_id_sm})에 필수 컬럼이 없습니다: {missing}.")
            st.error("코드 상단의 현재고 관련 상수(CURRENT_STOCK_..._COL)와 실제 엑셀 파일의 컬럼명을 확인해주세요.")
            return pd.DataFrame()

        df_stock_raw[CURRENT_STOCK_PROD_CODE_COL] = df_stock_raw[CURRENT_STOCK_PROD_CODE_COL].astype(str).str.strip()
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
            st.warning(f"현재고 데이터 그룹핑 후 데이터가 없습니다 ({latest_date_str}, ID: {file_id_sm}).")
            return pd.DataFrame()
        return current_stock_by_item_loc
    except ValueError as ve:
        if latest_date_str and f"Worksheet named '{latest_date_str}' not found" in str(ve):
            st.error(f"오류: 현재고 파일 (ID: {file_id_sm})에 '{latest_date_str}' 시트 없음")
        else:
            st.error(f"현재고 데이터 (ID: {file_id_sm}) 로드 중 값 오류: {ve}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"현재고 데이터 (ID: {file_id_sm}, 시트: {latest_date_str}) 로드/처리 중 예외 발생: {e}")
        st.error(traceback.format_exc()) # 상세 오류 로그 추가
        return pd.DataFrame()

# --- Streamlit 페이지 UI 및 로직 ---
st.title("📦 재고 보충 제안 보고서 (지점별)")

if drive_service is None: 
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()

# <<< 변경점: 필터링 조건 설명 추가 ---
MIN_SALES_DAYS_PER_MONTH = 5 
st.markdown(f"""
최근 3개월간의 월평균 출고량과 현재고를 **지점별로** 비교하여 보충 필요 수량을 제안합니다.
**단, 월평균 출고일수가 {MIN_SALES_DAYS_PER_MONTH}일 이상인 품목만 대상**으로 합니다.
""")
# --- 변경점 끝 ---

st.markdown(f"매출 데이터 원본: Google Drive 파일 (ID: `{SALES_FILE_ID}`)의 '{SALES_DATA_SHEET_NAME}' 시트")
st.markdown(f"현재고 데이터 원본: Google Drive 파일 (ID: `{SM_FILE_ID}`)의 최신 날짜 시트")
st.markdown("---")

num_months_to_analyze = 3
df_total_sales_3m = load_sales_history_and_filter_3m(drive_service, SALES_FILE_ID, SALES_DATA_SHEET_NAME, num_months=num_months_to_analyze)
df_current_stock = load_current_stock_data(drive_service, SM_FILE_ID)

if df_total_sales_3m.empty or df_current_stock.empty:
    st.warning("매출 데이터 또는 현재고 데이터가 없어 보고서를 생성할 수 없습니다. 위의 로그 메시지를 확인해주세요.")
else:
    df_avg_monthly_sales = df_total_sales_3m.copy()
    
    # <<< 변경점: 월평균 출고량 및 월평균 출고일수 계산 ---
    df_avg_monthly_sales['월평균 출고량(박스)'] = (df_avg_monthly_sales['TotalQtyBox'] / num_months_to_analyze).round(2)
    df_avg_monthly_sales['월평균 출고량(Kg)'] = (df_avg_monthly_sales['TotalQtyKg'] / num_months_to_analyze).round(2)
    # 'SalesDays'는 num_months_to_analyze 기간 동안의 총 출고일수이므로, 월평균 출고일수로 변환
    df_avg_monthly_sales['월평균 출고일수'] = (df_avg_monthly_sales['SalesDays'] / num_months_to_analyze).round(2)
    # --- 변경점 끝 ---

    # <<< 변경점: 월평균 출고일수 기준으로 필터링 ---
    df_avg_monthly_sales_filtered = df_avg_monthly_sales[df_avg_monthly_sales['월평균 출고일수'] >= MIN_SALES_DAYS_PER_MONTH].copy()
    
    if df_avg_monthly_sales_filtered.empty:
        st.warning(f"월평균 출고일수가 {MIN_SALES_DAYS_PER_MONTH}일 이상인 품목이 없습니다. 보고서를 생성할 수 없습니다.")
        st.stop() # 필터링 후 데이터 없으면 중단
    else:
        st.success(f"총 {len(df_avg_monthly_sales)}개 품목(지점별) 중 월평균 출고일수 {MIN_SALES_DAYS_PER_MONTH}일 이상인 {len(df_avg_monthly_sales_filtered)}개 품목을 대상으로 분석합니다.")
    # --- 변경점 끝 ---
    
    # 필터링된 데이터프레임으로 계속 진행
    df_avg_monthly_sales_to_use = df_avg_monthly_sales_filtered.rename(columns={
        SALES_PROD_CODE_COL: '상품코드', 
        SALES_PROD_NAME_COL: '상품명',
        SALES_LOCATION_COL: '지점명'
    })
    df_avg_monthly_sales_to_use['상품코드'] = df_avg_monthly_sales_to_use['상품코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_avg_monthly_sales_to_use['지점명'] = df_avg_monthly_sales_to_use['지점명'].astype(str).str.strip()
    # '월평균 출고일수'는 필터링 조건으로만 사용하고 최종 보고서에서는 제외할 수 있음 (필요시 포함)
    df_avg_monthly_sales_to_use = df_avg_monthly_sales_to_use[['상품코드', '상품명', '지점명', '월평균 출고량(박스)', '월평균 출고량(Kg)', '월평균 출고일수']]


    df_current_stock_report = df_current_stock.rename(columns={
        CURRENT_STOCK_PROD_CODE_COL: '상품코드',
        CURRENT_STOCK_PROD_NAME_COL: '상품명',
        CURRENT_STOCK_LOCATION_COL: '지점명',
        'CurrentQty': '잔량(박스)',
        'CurrentWgt': '잔량(Kg)'
    })
    df_current_stock_report['상품코드'] = df_current_stock_report['상품코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_current_stock_report['지점명'] = df_current_stock_report['지점명'].astype(str).str.strip()
    df_current_stock_report = df_current_stock_report[['상품코드', '지점명', '상품명', '잔량(박스)', '잔량(Kg)']]

    df_report = pd.merge(
        df_avg_monthly_sales_to_use, 
        df_current_stock_report, 
        on=['상품코드', '지점명'], 
        how='left', # 월평균 출고일수 기준을 통과한 품목 기준으로 재고를 붙임
        suffixes=('_sales', '_stock') 
    )
        
    df_report['상품명'] = df_report['상품명_sales'].fillna(df_report['상품명_stock'])
    df_report.drop(columns=['상품명_sales', '상품명_stock'], inplace=True, errors='ignore')
    
    df_report['잔량(박스)'] = df_report['잔량(박스)'].fillna(0)
    df_report['잔량(Kg)'] = df_report['잔량(Kg)'].fillna(0)

    df_report['필요수량(박스)'] = (df_report['월평균 출고량(박스)'] - df_report['잔량(박스)']).apply(lambda x: max(0, x)).round(2)
    df_report['필요수량(Kg)'] = (df_report['월평균 출고량(Kg)'] - df_report['잔량(Kg)']).apply(lambda x: max(0, x)).round(2)
    
    final_report_columns = [
        '지점명', '상품코드', '상품명', 
        '잔량(박스)', '잔량(Kg)', 
        '월평균 출고량(박스)', '월평균 출고량(Kg)',
        '월평균 출고일수', # <<< 필요시 최종 보고서에 포함 (확인용)
        '필요수량(박스)', '필요수량(Kg)'
    ]
    existing_final_cols = [col for col in final_report_columns if col in df_report.columns]
    df_report_final = df_report[existing_final_cols]

    df_report_final = df_report_final.sort_values(by=['지점명', '필요수량(박스)'], ascending=[True, False])

    st.markdown("---")
    st.header("📋 재고 보충 제안 리스트 (지점별)")
    
    df_display = df_report_final.copy()

    if '상품코드' in df_display.columns:
        try:
            df_display['상품코드'] = df_display['상품코드'].astype(str).str.replace(r'\.0$', '', regex=True)
        except Exception as e:
            st.warning(f"상품코드 문자열 변환 중 경미한 오류: {e}")
            df_display['상품코드'] = df_display['상품코드'].astype(str)

    cols_to_make_int_for_display = ['월평균 출고량(박스)', '필요수량(박스)', '잔량(박스)']
    for col in cols_to_make_int_for_display:
        if col in df_display.columns:
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce').fillna(0).round(0).astype('Int64')

    format_dict = {}
    for col in ['잔량(박스)', '월평균 출고량(박스)', '필요수량(박스)']:
        if col in df_display.columns: 
            format_dict[col] = "{:,.0f}"
    
    for col in ['잔량(Kg)', '월평균 출고량(Kg)', '필요수량(Kg)']:
        if col in df_display.columns: 
            format_dict[col] = "{:,.2f}"
    
    if '월평균 출고일수' in df_display.columns: # 월평균 출고일수도 소수점 2자리로 포맷
        format_dict['월평균 출고일수'] = "{:,.2f}"
        
    st.dataframe(df_display.style.format(format_dict, na_rep="-").set_properties(**{'text-align': 'right'}), use_container_width=True)

    @st.cache_data
    def convert_df_to_excel(df_to_convert):
        excel_stream = io.BytesIO()
        with pd.ExcelWriter(excel_stream, engine='xlsxwriter') as writer: 
            df_to_convert.to_excel(writer, index=False, sheet_name='보고서')
        excel_stream.seek(0)
        return excel_stream.getvalue()

    if not df_display.empty:
        excel_data = convert_df_to_excel(df_display)
        report_date_str = datetime.date.today().strftime("%Y%m%d")
        st.download_button(
            label="📥 보고서 엑셀로 다운로드",
            data=excel_data,
            file_name=f"재고보충제안보고서_지점별_{report_date_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_replenishment_report_formatted_page_filtered" # 키 변경
        )
