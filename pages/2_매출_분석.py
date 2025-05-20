# pages/2_매출_분석.py (검색 시 regex=False 추가 및 Cloud용 수정)

import streamlit as st
import pandas as pd
import datetime
# import os # os.path.exists는 더 이상 직접 사용하지 않음
import traceback
# import numpy as np # 현재 코드에서 직접 사용되지 않음

# common_utils.py 에서 공통 유틸리티 함수 가져오기
from common_utils import download_excel_from_drive_as_bytes

# --- Google Drive 파일 ID 정의 ---
# 사용자님이 제공해주신 실제 파일 ID를 사용합니다.
SALES_FILE_ID = "1h-V7kIoInXgGLll7YBW5V_uZdF3Q1PdY" # 매출내역 파일 ID
# --- 파일 ID 정의 끝 ---

# --- 이 페이지 고유의 설정 ---
SALES_SHEET_NAME = 's-list' # 매출내역 파일의 시트 이름

# 컬럼명 상수 (제공된 코드 기준)
DATE_COL = '매출일자'
AMOUNT_COL = '매출금액'
WEIGHT_COL = '수량(Kg)'
CUSTOMER_COL = '거래처명'
PRODUCT_COL = '상  품  명' # 원본 파일의 상품명 컬럼 (공백 2칸 포함 가능성 있음)
PRICE_COL = '매출단가'


# --- Google Drive 서비스 객체 가져오기 ---
retrieved_drive_service = st.session_state.get('drive_service')
page_title_for_debug = "매출 분석 페이지" 

if retrieved_drive_service:
    st.sidebar.info(f"'{page_title_for_debug}'에서 Drive Service 로드 성공!")
else:
    st.sidebar.error(f"'{page_title_for_debug}'에서 Drive Service 로드 실패! (None). 메인 페이지를 먼저 방문하여 인증을 완료해주세요.")

drive_service = retrieved_drive_service


@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None}) # drive_service 해시 방지
def load_sales_data(_drive_service, file_id_sales, sheet_name):
    """매출 로그 데이터를 Google Drive에서 로드하고 기본 전처리를 수행합니다."""
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (매출 데이터 로딩)")
        return None

    file_bytes_sales = download_excel_from_drive_as_bytes(_drive_service, file_id_sales, f"매출내역 ({sheet_name})")
    if file_bytes_sales is None:
        # download_excel_from_drive_as_bytes 함수 내에서 이미 st.error를 호출함
        return None
        
    try:
        required_cols = [DATE_COL, AMOUNT_COL, WEIGHT_COL, CUSTOMER_COL, PRODUCT_COL, PRICE_COL]
        df = pd.read_excel(file_bytes_sales, sheet_name=sheet_name)
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            st.error(f"오류: 매출 내역 시트 '{sheet_name}'에 필요한 컬럼({missing_cols}) 없음")
            st.write(f"사용 가능한 컬럼: {df.columns.tolist()}")
            return None
            
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')
        df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors='coerce').fillna(0)
        df[WEIGHT_COL] = pd.to_numeric(df[WEIGHT_COL], errors='coerce').fillna(0)
        df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors='coerce').fillna(0)
        df[CUSTOMER_COL] = df[CUSTOMER_COL].astype(str).str.strip()
        df[PRODUCT_COL] = df[PRODUCT_COL].astype(str).str.strip() # 상품명 컬럼 공백 제거
            
        original_rows = len(df)
        df.dropna(subset=[DATE_COL], inplace=True) # 날짜 누락 행 제거
        if len(df) < original_rows: 
            st.warning(f"'{DATE_COL}' 형식이 잘못되었거나 비어있는 {original_rows - len(df)}개 행이 제외되었습니다.")
        
        if df.empty:
            st.warning("전처리 후 남은 매출 데이터가 없습니다.")
            return pd.DataFrame() # 빈 DataFrame 반환
        return df
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve): 
            st.error(f"오류: 매출 내역 파일 (ID: {file_id_sales})에 '{sheet_name}' 시트 없음")
        else: 
            st.error(f"매출 데이터 (ID: {file_id_sales}, 시트: {sheet_name}) 로드 중 값 오류: {ve}")
        return None
    except Exception as e: 
        st.error(f"매출 데이터 (ID: {file_id_sales}, 시트: {sheet_name}) 로드 중 예상 못한 오류: {e}")
        # traceback.print_exc() # 디버깅 시 필요하면 주석 해제
        return None

# --- Streamlit 페이지 구성 ---
st.title("📈 매출 분석")
st.markdown("---")

if drive_service is None: 
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()

df_sales_loaded = load_sales_data(drive_service, SALES_FILE_ID, SALES_SHEET_NAME)

if df_sales_loaded is None:
    st.error("매출 데이터를 불러오지 못했습니다. Google Drive 파일 ID, 시트 이름, 파일 내용을 확인해주세요.")
elif df_sales_loaded.empty:
    st.warning("처리할 매출 데이터가 없습니다 (파일은 읽었으나 내용이 비어있거나 모두 필터링됨).")
else:
    st.success(f"매출 데이터 로드 및 기본 전처리 완료: {len(df_sales_loaded)} 행")
    today = pd.Timestamp.today().normalize()
    # 기본 분석 기간을 로드된 데이터의 최근 날짜를 기준으로 설정하도록 변경 고려 가능
    # 여기서는 기존 로직(오늘 기준 최근 90일)을 유지하되, 데이터가 없을 경우를 대비
    
    min_data_date = df_sales_loaded[DATE_COL].min()
    max_data_date = df_sales_loaded[DATE_COL].max()

    # 날짜 선택 UI 개선: 데이터가 있는 범위 내에서 선택하도록 유도
    date_range_col1, date_range_col2 = st.columns(2)
    with date_range_col1:
        start_date_input = st.date_input(
            "분석 시작일", 
            value=max_data_date - pd.Timedelta(days=89) if not pd.isna(max_data_date) else today - pd.Timedelta(days=89),
            min_value=min_data_date if not pd.isna(min_data_date) else None,
            max_value=max_data_date if not pd.isna(max_data_date) else today,
            key="sales_start_date"
        )
    with date_range_col2:
        end_date_input = st.date_input(
            "분석 종료일", 
            value=max_data_date if not pd.isna(max_data_date) else today,
            min_value=start_date_input if start_date_input else (min_data_date if not pd.isna(min_data_date) else None),
            max_value=max_data_date if not pd.isna(max_data_date) else today,
            key="sales_end_date"
        )
    
    start_date = pd.Timestamp(start_date_input)
    end_date = pd.Timestamp(end_date_input)

    st.info(f"선택된 분석 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    df_filtered_global = df_sales_loaded[
        (df_sales_loaded[DATE_COL] >= start_date) & 
        (df_sales_loaded[DATE_COL] <= end_date)
    ].copy()

    if df_filtered_global.empty:
        st.warning("선택된 기간 내에 해당하는 매출 데이터가 없습니다.")
    else:
        col1, col2 = st.columns([2, 3]) # 레이아웃 비율 조정

        with col2: # 검색 조건 및 상세 내역
            st.header("🔍 조건별 매출 상세 조회")
            st.markdown("거래처명 또는 품목명(일부 또는 전체)을 입력하여 선택된 기간의 상세 매출 내역 및 관련 그래프를 조회합니다.")
            customer_input_raw = st.text_input("거래처명 검색:", key="sales_customer_input")
            product_input_raw = st.text_input("품목명 검색:", key="sales_product_input")

            customer_input = customer_input_raw.strip()
            product_input = product_input_raw.strip()

            df_for_display = df_filtered_global.copy() # 검색을 위해 원본 필터된 데이터 복사
            filter_active = False
            active_filters = []

            if customer_input:
                df_for_display = df_for_display[df_for_display[CUSTOMER_COL].str.contains(customer_input, case=False, na=False, regex=False)]
                filter_active = True
                active_filters.append(f"거래처: '{customer_input}'")
            if product_input:
                df_for_display = df_for_display[df_for_display[PRODUCT_COL].str.contains(product_input, case=False, na=False, regex=False)]
                filter_active = True
                active_filters.append(f"품목: '{product_input}'")
            
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
                    st.dataframe(df_display_detail, hide_index=True, use_container_width=True, height=300) # 높이 지정
                else:
                    st.info("해당 검색 조건에 맞는 상세 내역이 없습니다.")
            elif not customer_input_raw and not product_input_raw:
                st.info("거래처명 또는 품목명을 입력하고 Enter를 누르면 해당 조건의 상세 내역 및 그래프를 조회합니다.")
        
        with col1: # 그래프 표시
            graph_title_suffix = ""
            if filter_active:
                graph_title_suffix = f" ({', '.join(active_filters)})"
            
            st.header(f"📊 일별 매출 추이{graph_title_suffix}")
            if not filter_active :
                st.markdown(f"선택된 기간({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})의 전체 일별 매출 금액과 판매 중량(Kg) 추세입니다.")
            else:
                st.markdown(f"검색 조건에 따른 선택된 기간의 일별 매출 금액과 판매 중량(Kg) 추세입니다.")

            if df_for_display.empty:
                st.warning("선택된 조건에 해당하는 매출 데이터가 없어 그래프를 표시할 수 없습니다.")
            else:
                daily_summary = df_for_display.groupby(pd.Grouper(key=DATE_COL, freq='D'))[[AMOUNT_COL, WEIGHT_COL]].sum()
                # 그래프 표시 전, 합계가 0인 날짜는 제외 (선택 사항)
                daily_summary_for_chart = daily_summary[~((daily_summary[AMOUNT_COL] == 0) & (daily_summary[WEIGHT_COL] == 0))]
                
                if daily_summary_for_chart.empty:
                    st.write("그래프에 표시할 데이터가 없습니다 (모든 날짜의 합계가 0이거나 데이터 없음).")
                else:
                    daily_summary_for_chart = daily_summary_for_chart.copy() # SettingWithCopyWarning 방지
                    # 컬럼 이름 변경 시, 원본 WEIGHT_COL 상수 값을 포함하여 명확히 함
                    daily_summary_for_chart.rename(columns={AMOUNT_COL: '매출 금액(원)', WEIGHT_COL: f'판매 중량({WEIGHT_COL})'}, inplace=True)

                    st.subheader("금액 (원)")
                    st.line_chart(daily_summary_for_chart[['매출 금액(원)']], use_container_width=True)

                    st.subheader(f"중량 ({WEIGHT_COL})") # WEIGHT_COL 상수 사용
                    st.line_chart(daily_summary_for_chart[[f'판매 중량({WEIGHT_COL})']], use_container_width=True)

                with st.expander("선택 조건 일별 요약 데이터 보기"):
                    # 검색 조건이 적용된 df_for_display를 사용
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
