# pages/4_재고_보충_제안.py

import streamlit as st
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os
import io # BytesIO 사용을 위해 필요
# common_utils.py 에서 공통 파일 경로 및 유틸리티 함수 가져오기
from common_utils import DATA_FOLDER, SM_FILE, get_all_available_sheet_dates

# --- 보고서에 사용할 상수 ---
SALES_DATA_FILE_PATH = os.path.join(DATA_FOLDER, '매출내역.xlsx')
SALES_DATA_SHEET_NAME = 's-list'

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


@st.cache_data
def load_sales_history_and_filter_3m(filepath, sheet_name, num_months=3):
    """
    지정된 엑셀 파일/시트에서 전체 매출 데이터를 로드하고,
    지난 N개의 완전한 달력 월 데이터를 필터링하여 [상품코드, 상품명, 지점명]별 총 출고량을 반환합니다.
    """
    try:
        if not os.path.exists(filepath):
            st.error(f"오류: 매출 파일 '{os.path.basename(filepath)}' 없음")
            return pd.DataFrame()

        required_cols = [SALES_DATE_COL, SALES_PROD_CODE_COL, SALES_PROD_NAME_COL, 
                         SALES_QTY_BOX_COL, SALES_QTY_KG_COL, SALES_LOCATION_COL]
        
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        # st.info(f"'{os.path.basename(filepath)}' ({sheet_name}) 시트 원본 데이터 로드: {len(df)} 행") # 사용자에게 필요한 정보면 유지

        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            st.error(f"오류: 매출 시트에 필요한 컬럼({missing_cols}) 없음. 상수 또는 파일 내 컬럼명을 확인하세요.")
            return pd.DataFrame()

        df[SALES_DATE_COL] = pd.to_datetime(df[SALES_DATE_COL], errors='coerce')
        df.dropna(subset=[SALES_DATE_COL], inplace=True)
        
        df[SALES_PROD_CODE_COL] = df[SALES_PROD_CODE_COL].astype(str).str.strip()
        df[SALES_PROD_NAME_COL] = df[SALES_PROD_NAME_COL].astype(str).str.strip()
        df[SALES_LOCATION_COL] = df[SALES_LOCATION_COL].astype(str).str.strip()
        df[SALES_QTY_BOX_COL] = pd.to_numeric(df[SALES_QTY_BOX_COL], errors='coerce').fillna(0)
        df[SALES_QTY_KG_COL] = pd.to_numeric(df[SALES_QTY_KG_COL], errors='coerce').fillna(0)

        if df.empty:
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
            st.warning(f"선택된 기간 ({start_date_of_analysis_period.strftime('%Y-%m-%d')} ~ {end_date_of_last_full_month.strftime('%Y-%m-%d')})의 매출 데이터가 '{sheet_name}' 시트에 없습니다.")
            return pd.DataFrame()
        
        st.success(f"필터링된 매출 데이터: {len(df_filtered)} 행")

        total_sales_by_item_loc = df_filtered.groupby(
            [SALES_PROD_CODE_COL, SALES_PROD_NAME_COL, SALES_LOCATION_COL], 
            as_index=False
        ).agg(
            TotalQtyBox=(SALES_QTY_BOX_COL, 'sum'),
            TotalQtyKg=(SALES_QTY_KG_COL, 'sum')
        )
        return total_sales_by_item_loc
    except FileNotFoundError:
        st.error(f"오류: 매출 파일 '{os.path.basename(filepath)}' 없음")
        return pd.DataFrame()
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve):
            st.error(f"오류: 매출 파일 '{os.path.basename(filepath)}'에 '{sheet_name}' 시트 없음")
        else:
            st.error(f"매출 데이터 로드 중 값 오류: {ve}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"매출 데이터 로드/처리 중 예상 못한 오류: {e}")
        return pd.DataFrame()

@st.cache_data
def load_current_stock_data(sm_filepath):
    available_sm_dates = get_all_available_sheet_dates(sm_filepath) 
    if not available_sm_dates:
        st.warning(f"'{os.path.basename(sm_filepath)}'에서 사용 가능한 재고 데이터 시트를 찾을 수 없습니다.")
        return pd.DataFrame()

    latest_date_obj = available_sm_dates[0]
    latest_date_str = latest_date_obj.strftime("%Y%m%d")
    st.info(f"현재고 기준일: {latest_date_obj.strftime('%Y-%m-%d')} (시트: {latest_date_str})")

    try:
        df_stock_raw = pd.read_excel(sm_filepath, sheet_name=latest_date_str)
        
        required_stock_cols = [CURRENT_STOCK_PROD_CODE_COL, CURRENT_STOCK_PROD_NAME_COL, 
                               CURRENT_STOCK_QTY_COL, CURRENT_STOCK_WGT_COL, CURRENT_STOCK_LOCATION_COL]
        
        if not all(col in df_stock_raw.columns for col in required_stock_cols):
            missing = [col for col in required_stock_cols if col not in df_stock_raw.columns]
            st.error(f"현재고 데이터 시트({latest_date_str})에 필수 컬럼이 없습니다: {missing}.")
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
            st.warning(f"현재고 데이터 그룹핑 후 데이터가 없습니다 ({latest_date_str}).")
            return pd.DataFrame()

        st.success(f"현재고 데이터 처리 완료: {len(current_stock_by_item_loc)}개 품목(지점별).")
        return current_stock_by_item_loc
    except Exception as e:
        st.error(f"현재고 데이터 로드/처리 중 예외 발생 ({latest_date_str}): {e}")
        return pd.DataFrame()

# --- Streamlit 페이지 UI 및 로직 ---
st.title("📦 재고 보충 제안 보고서 (지점별)")
st.markdown("최근 3개월간의 월평균 출고량과 현재고를 **지점별로** 비교하여 보충 필요 수량을 제안합니다.")
st.markdown(f"매출 데이터 원본: '{os.path.basename(SALES_DATA_FILE_PATH)}' 파일의 '{SALES_DATA_SHEET_NAME}' 시트")
st.markdown(f"현재고 데이터 원본: '{os.path.basename(SM_FILE)}' 파일의 최신 날짜 시트")
st.markdown("---")

num_months_to_analyze = 3
df_total_sales_3m = load_sales_history_and_filter_3m(SALES_DATA_FILE_PATH, SALES_DATA_SHEET_NAME, num_months=num_months_to_analyze)
df_current_stock = load_current_stock_data(SM_FILE)

if df_total_sales_3m.empty or df_current_stock.empty:
    st.warning("매출 데이터 또는 현재고 데이터가 없어 보고서를 생성할 수 없습니다. 위의 로그 메시지를 확인해주세요.")
else:
    df_avg_monthly_sales = df_total_sales_3m.copy()
    df_avg_monthly_sales['월평균 출고량(박스)'] = (df_avg_monthly_sales['TotalQtyBox'] / num_months_to_analyze).round(2)
    df_avg_monthly_sales['월평균 출고량(Kg)'] = (df_avg_monthly_sales['TotalQtyKg'] / num_months_to_analyze).round(2)
    
    df_avg_monthly_sales.rename(columns={
        SALES_PROD_CODE_COL: '상품코드', 
        SALES_PROD_NAME_COL: '상품명',
        SALES_LOCATION_COL: '지점명'
    }, inplace=True)
    df_avg_monthly_sales['상품코드'] = df_avg_monthly_sales['상품코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_avg_monthly_sales['지점명'] = df_avg_monthly_sales['지점명'].astype(str).str.strip()
    df_avg_monthly_sales = df_avg_monthly_sales[['상품코드', '상품명', '지점명', '월평균 출고량(박스)', '월평균 출고량(Kg)']]

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
        df_avg_monthly_sales, 
        df_current_stock_report, 
        on=['상품코드', '지점명'], 
        how='left',
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
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce').round(0).astype('Int64')

    format_dict = {}
    for col in ['잔량(박스)', '월평균 출고량(박스)', '필요수량(박스)']:
        if col in df_display.columns: 
            format_dict[col] = "{:,.0f}"
    
    for col in ['잔량(Kg)', '월평균 출고량(Kg)', '필요수량(Kg)']:
        if col in df_display.columns: 
            format_dict[col] = "{:,.2f}"
    
    st.dataframe(df_display.style.format(format_dict, na_rep="-").set_properties(**{'text-align': 'right'}), use_container_width=True)

    @st.cache_data
    def convert_df_to_excel(df_to_convert):
        from io import BytesIO
        excel_stream = BytesIO()
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
            key="download_replenishment_report_formatted_page"
        )
