# pages/2_매출_분석.py (검색 시 regex=False 추가)

import streamlit as st
import pandas as pd
import datetime
import os
import traceback
# import numpy as np # 현재 코드에서 직접 사용되지 않음

# --- 설정 (common_utils.py에서 가져오거나 여기서 직접 정의) ---
DATA_FOLDER = r"C:\Users\kmeat 1f\Documents\googleaiy\list" # 실제 경로로 수정 필요
SALES_FILE = os.path.join(DATA_FOLDER, '매출내역.xlsx') 
SALES_SHEET_NAME = 's-list'

# 컬럼명 상수 (제공된 코드 기준)
DATE_COL = '매출일자'
AMOUNT_COL = '매출금액'
WEIGHT_COL = '수량(Kg)'
CUSTOMER_COL = '거래처명'
PRODUCT_COL = '상  품  명' # 공백 2칸짜리 이름
PRICE_COL = '매출단가'


@st.cache_data
def load_sales_data(filepath, sheet_name):
    """매출 로그 데이터를 로드하고 기본 전처리를 수행합니다."""
    try:
        if not os.path.exists(filepath): 
            st.error(f"오류: 매출 내역 파일 '{os.path.basename(filepath)}' 없음"); return None
        required_cols = [DATE_COL, AMOUNT_COL, WEIGHT_COL, CUSTOMER_COL, PRODUCT_COL, PRICE_COL]
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            st.error(f"오류: 매출 내역 시트 '{sheet_name}'에 필요한 컬럼({missing_cols}) 없음")
            st.write(f"사용 가능한 컬럼: {df.columns.tolist()}"); return None
        
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')
        df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors='coerce').fillna(0)
        df[WEIGHT_COL] = pd.to_numeric(df[WEIGHT_COL], errors='coerce').fillna(0)
        df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors='coerce').fillna(0)
        df[CUSTOMER_COL] = df[CUSTOMER_COL].astype(str).str.strip()
        df[PRODUCT_COL] = df[PRODUCT_COL].astype(str).str.strip()
        
        original_rows = len(df)
        df.dropna(subset=[DATE_COL], inplace=True)
        if len(df) < original_rows: 
            st.warning(f"'{DATE_COL}' 형식이 잘못되었거나 비어있는 {original_rows - len(df)}개 행이 제외되었습니다.")
        
        if df.empty:
            st.warning("전처리 후 남은 매출 데이터가 없습니다.")
            return None
        return df
    except FileNotFoundError: 
        st.error(f"오류: 매출 내역 파일 '{os.path.basename(filepath)}' 없음"); return None
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve): 
            st.error(f"오류: 매출 내역 파일 '{os.path.basename(filepath)}'에 '{sheet_name}' 시트 없음")
        else: 
            st.error(f"매출 데이터 로드 중 값 오류: {ve}")
        return None
    except Exception as e: 
        st.error(f"매출 데이터 로드 중 예상 못한 오류: {e}"); traceback.print_exc(); return None

# --- Streamlit 페이지 구성 ---
st.title("📈 매출 분석")
st.markdown("---")

df_sales_loaded = load_sales_data(SALES_FILE, SALES_SHEET_NAME)

if df_sales_loaded is None:
    st.error("매출 데이터를 불러오지 못했습니다. 파일 경로, 시트 이름, 파일 내용을 확인해주세요.")
elif df_sales_loaded.empty:
    st.warning("처리할 매출 데이터가 없습니다 (파일은 읽었으나 내용이 비어있거나 모두 필터링됨).")
else:
    st.success(f"매출 데이터 로드 및 기본 전처리 완료: {len(df_sales_loaded)} 행")
    today = pd.Timestamp.today().normalize()
    start_date = today - pd.Timedelta(days=89) 
    st.info(f"기본 분석 기간: {start_date.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')} (최근 90일)")
    
    df_filtered_global = df_sales_loaded[
        (df_sales_loaded[DATE_COL] >= start_date) & 
        (df_sales_loaded[DATE_COL] <= today)
    ].copy()

    if df_filtered_global.empty:
        st.warning("선택된 기간(최근 90일) 내에 해당하는 매출 데이터가 없습니다.")
    else:
        col1, col2 = st.columns([2, 3])

        with col2:
            st.header("🔍 조건별 매출 상세 조회")
            st.markdown("거래처명 또는 품목명(일부 또는 전체)을 입력하여 최근 90일간의 상세 매출 내역 및 관련 그래프를 조회합니다.")
            customer_input_raw = st.text_input("거래처명 검색:", key="sales_customer_input")
            product_input_raw = st.text_input("품목명 검색:", key="sales_product_input")

            # 입력값 앞뒤 공백 제거
            customer_input = customer_input_raw.strip()
            product_input = product_input_raw.strip()

        df_for_display = df_filtered_global
        filter_active = False
        active_filters = []

        if customer_input:
            # regex=False 추가하여 일반 텍스트로 검색
            df_for_display = df_for_display[df_for_display[CUSTOMER_COL].str.contains(customer_input, case=False, na=False, regex=False)]
            filter_active = True
            active_filters.append(f"거래처: '{customer_input}'")
        if product_input:
            # regex=False 추가하여 일반 텍스트로 검색
            df_for_display = df_for_display[df_for_display[PRODUCT_COL].str.contains(product_input, case=False, na=False, regex=False)]
            filter_active = True
            active_filters.append(f"품목: '{product_input}'")
        
        with col1:
            graph_title_suffix = ""
            if filter_active:
                graph_title_suffix = f" ({', '.join(active_filters)})"
            
            st.header(f"📊 일별 매출 추이{graph_title_suffix}")
            if not filter_active :
                 st.markdown("최근 90일간의 전체 일별 매출 금액과 판매 중량(Kg) 추세입니다.")
            else:
                 st.markdown(f"검색 조건에 따른 최근 90일간의 일별 매출 금액과 판매 중량(Kg) 추세입니다.")

            if df_for_display.empty:
                st.warning("선택된 조건에 해당하는 매출 데이터가 없어 그래프를 표시할 수 없습니다.")
            else:
                daily_summary = df_for_display.groupby(pd.Grouper(key=DATE_COL, freq='D'))[[AMOUNT_COL, WEIGHT_COL]].sum()
                daily_summary_for_chart = daily_summary[~((daily_summary[AMOUNT_COL] == 0) & (daily_summary[WEIGHT_COL] == 0))]
                
                if daily_summary_for_chart.empty:
                     st.write("그래프에 표시할 데이터가 없습니다 (모든 날짜의 합계가 0).")
                else:
                    daily_summary_for_chart = daily_summary_for_chart.copy()
                    daily_summary_for_chart.rename(columns={AMOUNT_COL: '매출 금액(원)', WEIGHT_COL: f'판매 중량({WEIGHT_COL})'}, inplace=True)

                    st.subheader("금액 (원)")
                    st.line_chart(daily_summary_for_chart[['매출 금액(원)']], use_container_width=True)

                    st.subheader(f"중량 ({WEIGHT_COL})")
                    st.line_chart(daily_summary_for_chart[[f'판매 중량({WEIGHT_COL})']], use_container_width=True)

                with st.expander("선택 조건 일별 요약 데이터 보기"):
                    daily_summary_table_data = df_for_display.groupby(pd.Grouper(key=DATE_COL, freq='D'))[[AMOUNT_COL, WEIGHT_COL]].sum().reset_index()
                    if daily_summary_table_data.empty:
                        st.write("요약할 데이터가 없습니다.")
                    else:
                        weekday_map = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}
                        daily_summary_table_data['요일'] = daily_summary_table_data[DATE_COL].dt.dayofweek.map(weekday_map)
                        daily_summary_table_data[DATE_COL] = daily_summary_table_data[DATE_COL].dt.strftime('%Y-%m-%d')
                        daily_summary_table_data.rename(columns={AMOUNT_COL: '매출 금액(원)', WEIGHT_COL: f'판매 중량({WEIGHT_COL})'}, inplace=True)
                        
                        display_columns = [DATE_COL, '요일', '매출 금액(원)', f'판매 중량({WEIGHT_COL})']
                        st.dataframe(daily_summary_table_data[display_columns], use_container_width=True, hide_index=True)
        
        with col2:
            if filter_active:
                st.markdown("---")
                st.subheader(f"'{' / '.join(active_filters) if active_filters else '전체'}' 상세 검색 결과")
                st.write(f"총 {len(df_for_display)} 건의 매출 내역이 검색되었습니다.")
                if not df_for_display.empty:
                    display_cols_detail = [DATE_COL, CUSTOMER_COL, PRODUCT_COL, WEIGHT_COL, PRICE_COL, AMOUNT_COL]
                    valid_display_cols_detail = [col for col in display_cols_detail if col in df_for_display.columns]
                    df_display_detail = df_for_display[valid_display_cols_detail].copy()
                    
                    df_display_detail[DATE_COL] = df_display_detail[DATE_COL].dt.strftime('%Y-%m-%d')
                    df_display_detail.sort_values(by=DATE_COL, ascending=False, inplace=True)
                    st.dataframe(df_display_detail, hide_index=True, use_container_width=True)
            elif not customer_input_raw and not product_input_raw: # 원본 입력값으로 판단
                st.info("거래처명 또는 품목명을 입력하고 Enter를 누르면 해당 조건의 상세 내역 및 그래프를 조회합니다.")
