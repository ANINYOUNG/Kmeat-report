
# pages/3_일일_재고_확인.py (장기 재고 현황에 입고당시 Box/Kg 수량 추가 및 Cloud용 수정, 입고번호 추가)

import streamlit as st
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
# import os # os.path.exists는 더 이상 직접 사용하지 않음
import traceback
import numpy as np # compare_inventories 함수에서 사용되었던 것처럼, 필요할 수 있음 (현재 코드에서는 직접 미사용)
import io # io.BytesIO 사용

# common_utils.py 에서 공통 유틸리티 함수 가져오기
from common_utils import download_excel_from_drive_as_bytes
# get_all_available_sheet_dates_from_bytes 함수는 이 파일의 find_latest_sheet와 유사/대체 가능

# --- Google Drive 파일 ID 정의 ---
# 사용자님이 제공해주신 실제 파일 ID를 사용합니다.
SM_FILE_ID = "1tRljdvOpp4fITaVEXvoL9mNveNg2qt4p" # SM재고현황 파일 ID
# --- 파일 ID 정의 끝 ---

# --- 이 페이지 고유의 설정 ---
RECEIPT_NUMBER_COL = '번호' # 입고번호 컬럼명 (SM재고현황 파일 기준)
PROD_CODE_COL = '상품코드'
PROD_NAME_COL = '상품명'
BRANCH_COL = '지점명'
QTY_COL = '잔량(박스)' 
WGT_COL = '잔량(Kg)' 
EXP_DATE_COL = '소비기한'
RECEIPT_DATE_COL = '입고일자'
INITIAL_QTY_BOX_COL = 'Box'      # 입고 당시 박스 수량 컬럼명 (SM재고 파일 기준)
INITIAL_QTY_KG_COL = '입고(Kg)'  # 입고 당시 Kg 수량 컬럼명 (SM재고 파일 기준)
REMAINING_DAYS_COL = '잔여일수'

# REQUIRED_COLS_FOR_PAGE에 RECEIPT_NUMBER_COL 추가
REQUIRED_COLS_FOR_PAGE = [RECEIPT_NUMBER_COL, PROD_CODE_COL, PROD_NAME_COL, BRANCH_COL, QTY_COL, WGT_COL,
                          EXP_DATE_COL, RECEIPT_DATE_COL, 
                          INITIAL_QTY_BOX_COL, INITIAL_QTY_KG_COL, 
                          REMAINING_DAYS_COL]

KEYWORD_REFRIGERATED = "냉장"
THRESHOLD_REFRIGERATED = 21
THRESHOLD_OTHER = 90

# --- Google Drive 서비스 객체 가져오기 ---
retrieved_drive_service = st.session_state.get('drive_service')
page_title_for_debug = "일일 재고 확인 페이지" 

if retrieved_drive_service:
    st.sidebar.info(f"'{page_title_for_debug}'에서 Drive Service 로드 성공!")
else:
    st.sidebar.error(f"'{page_title_for_debug}'에서 Drive Service 로드 실패! (None). 메인 페이지를 먼저 방문하여 인증을 완료해주세요.")

drive_service = retrieved_drive_service

# --- 함수 정의 (Google Drive 연동으로 수정) ---

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def find_latest_sheet(_drive_service, file_id_sm):
    """Google Drive의 Excel 파일에서 YYYYMMDD 형식의 가장 최신 날짜 시트 이름을 찾습니다."""
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (최신 시트 검색)")
        return None

    file_bytes_sm = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, "SM재고현황 (최신 시트 검색용)")
    if file_bytes_sm is None:
        return None # 오류 메시지는 download 함수에서 이미 표시됨
        
    try:
        with pd.ExcelFile(file_bytes_sm) as xls:
            sheet_names = xls.sheet_names
            # YYYYMMDD 형식의 시트 이름을 찾습니다. (예: 20230521)
            date_sheets = [name for name in sheet_names if len(name) == 8 and name.isdigit()]
            if not date_sheets: 
                st.error(f"오류: SM재고현황 파일 (ID: {file_id_sm})에 YYYYMMDD 형식 시트 없음")
                return None
            latest_sheet = max(date_sheets)
            return latest_sheet
    except Exception as e: 
        st.error(f"SM재고현황 파일 (ID: {file_id_sm}) 시트 목록 읽기 오류: {e}")
        return None

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_sm_sheet_for_daily_check(_drive_service, file_id_sm, sheet_name):
    """일일 확인용 SM 재고 시트를 Google Drive에서 로드하고 필요한 컬럼 확인 및 기본 처리합니다."""
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (일일 재고 데이터 로딩)")
        return None

    file_bytes_sm = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, f"SM재고현황 ({sheet_name})")
    if file_bytes_sm is None:
        return None

    try:
        # 메모리 최적화 제안: 만약 Excel 파일에 불필요한 컬럼이 많다면,
        # usecols 파라미터를 사용하여 필요한 컬럼만 로드하는 것을 고려할 수 있습니다.
        # 예: df = pd.read_excel(file_bytes_sm, sheet_name=sheet_name, usecols=REQUIRED_COLS_FOR_PAGE)
        # 단, REQUIRED_COLS_FOR_PAGE에 없는 컬럼을 참조하면 오류가 발생하므로 주의해야 합니다.
        # 현재 로직은 모든 컬럼을 읽은 후, 필요한 컬럼이 있는지 확인하고 없으면 생성/채우는 방식입니다.
        df = pd.read_excel(file_bytes_sm, sheet_name=sheet_name)

        # 필수 컬럼 존재 여부 확인 및 처리
        missing_cols = [col for col in REQUIRED_COLS_FOR_PAGE if col not in df.columns]
        if missing_cols:
            st.warning(f"SM 시트 '{sheet_name}'에 다음 필수 컬럼이 없습니다: {', '.join(missing_cols)}")
            # 누락된 필수 컬럼 중 특정 컬럼들은 기본값으로 채우는 로직
            if INITIAL_QTY_BOX_COL in missing_cols:
                st.info(f"'{INITIAL_QTY_BOX_COL}' 컬럼이 없어 0으로 채웁니다.")
                df[INITIAL_QTY_BOX_COL] = 0
                missing_cols.remove(INITIAL_QTY_BOX_COL)
            if INITIAL_QTY_KG_COL in missing_cols:
                st.info(f"'{INITIAL_QTY_KG_COL}' 컬럼이 없어 0으로 채웁니다.")
                df[INITIAL_QTY_KG_COL] = 0
                missing_cols.remove(INITIAL_QTY_KG_COL)
            
            if RECEIPT_NUMBER_COL in missing_cols:
                st.info(f"'{RECEIPT_NUMBER_COL}' 컬럼이 없어 빈 값으로 채웁니다.")
                df[RECEIPT_NUMBER_COL] = "" # 또는 pd.NA
                missing_cols.remove(RECEIPT_NUMBER_COL)
            
            # REMAINING_DAYS_COL 도 필요하다면 여기서 유사한 처리 추가 가능
            # if REMAINING_DAYS_COL in missing_cols:
            #     st.info(f"'{REMAINING_DAYS_COL}' 컬럼이 없어 NaN으로 채웁니다. (이후 소비기한 계산에서 파생될 수 있음)")
            #     df[REMAINING_DAYS_COL] = pd.NA # 또는 np.nan
            #     missing_cols.remove(REMAINING_DAYS_COL)

            if missing_cols: # 위에서 처리되지 않은 다른 필수 컬럼이 여전히 없다면
                st.error(f"분석에 필요한 나머지 필수 컬럼({', '.join(missing_cols)})도 없습니다.")
                st.write(f"사용 가능한 컬럼: {df.columns.tolist()}")
                return None
            
        df[RECEIPT_NUMBER_COL] = df.get(RECEIPT_NUMBER_COL, pd.Series(dtype='str')).fillna('').astype(str).str.strip()
        df[PROD_CODE_COL] = df[PROD_CODE_COL].fillna('').astype(str).str.replace(r'\.0$', '', regex=True)
        df[PROD_NAME_COL] = df[PROD_NAME_COL].astype(str).str.strip()
        df[BRANCH_COL] = df[BRANCH_COL].astype(str).str.strip()
        df[EXP_DATE_COL] = df[EXP_DATE_COL].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        df[RECEIPT_DATE_COL] = pd.to_datetime(df[RECEIPT_DATE_COL], errors='coerce')
        
        df[INITIAL_QTY_BOX_COL] = pd.to_numeric(df.get(INITIAL_QTY_BOX_COL, 0), errors='coerce').fillna(0)
        df[INITIAL_QTY_KG_COL] = pd.to_numeric(df.get(INITIAL_QTY_KG_COL, 0), errors='coerce').fillna(0)
        
        # REMAINING_DAYS_COL이 REQUIRED_COLS_FOR_PAGE에 있으므로, 존재한다고 가정하거나,
        # 여기서도 .get() 또는 컬럼 생성 로직이 필요할 수 있습니다.
        # 현재는 REQUIRED_COLS_FOR_PAGE에 포함되어 있으므로 df[REMAINING_DAYS_COL]로 직접 접근.
        df[REMAINING_DAYS_COL] = pd.to_numeric(df[REMAINING_DAYS_COL], errors='coerce') # NaN 가능
        df[QTY_COL] = pd.to_numeric(df[QTY_COL], errors='coerce').fillna(0)
        df[WGT_COL] = pd.to_numeric(df[WGT_COL], errors='coerce').fillna(0)

        # 메모리 최적화 제안: 데이터 로드 및 기본 처리 후,
        # 문자열 컬럼 중 고유값이 적은 경우 (예: 상품명, 지점명 등) category 타입으로 변경하면 메모리 절약 가능
        # 예: df[PROD_NAME_COL] = df[PROD_NAME_COL].astype('category')
        # df[BRANCH_COL] = df[BRANCH_COL].astype('category')
        # 숫자 컬럼도 가능한 경우 더 작은 타입으로 변경 (pd.to_numeric의 downcast 옵션 활용)

        return df
    except ValueError as ve:
        if sheet_name and f"Worksheet named '{sheet_name}' not found" in str(ve): 
            st.error(f"오류: SM 파일 (ID: {file_id_sm})에 '{sheet_name}' 시트 없음")
        else: 
            st.error(f"SM 데이터 (ID: {file_id_sm}, 시트: {sheet_name}) 로드/처리 중 값 오류: {ve}")
        return None
    except Exception as e: 
        st.error(f"SM 시트 (ID: {file_id_sm}, 시트: '{sheet_name}') 로드 중 예상 못한 오류: {e}")
        # traceback.print_exc() # 디버깅 시 상세 오류 출력
        return None

# --- Streamlit 페이지 구성 ---
# st.set_page_config(page_title="일일 재고 확인", layout="wide") # 메인 앱에서 한번만 호출
st.title("📋 일일 재고 확인")
st.markdown("---")

if drive_service is None: 
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()

st.markdown("SM 재고 데이터의 **가장 최신 날짜**를 기준으로 주요 확인 사항을 점검합니다.")

latest_sheet_name = find_latest_sheet(drive_service, SM_FILE_ID)

if latest_sheet_name:
    st.success(f"조회 대상 시트: '{latest_sheet_name}' (SM재고현황 파일 기준)")
    df_sm_latest_raw = load_sm_sheet_for_daily_check(drive_service, SM_FILE_ID, latest_sheet_name)

    if df_sm_latest_raw is not None and not df_sm_latest_raw.empty:
        st.success(f"데이터 로드 및 기본 처리 완료: {len(df_sm_latest_raw)} 행")
        # st.info(f"메모리 사용량: {df_sm_latest_raw.memory_usage(deep=True).sum() / (1024*1024):.2f} MB") # 디버깅용 메모리 사용량 확인
        st.markdown("---")
        col1, col2 = st.columns([1, 2]) # 레이아웃 비율

        with col1:
            st.header("⚠️ 소비기한 누락 품목")
            try:
                # 소비기한이 비어있거나, 'nan', 'NaT', 'None', 'nat' 문자열이거나, pd.isna로 True인 경우 필터링
                missing_exp_date_filter = df_sm_latest_raw[EXP_DATE_COL].astype(str).str.strip().isin(['', 'nan', 'NaT', 'None', 'nat']) | \
                                          pd.isna(df_sm_latest_raw[EXP_DATE_COL])
                # .copy()는 SettingWithCopyWarning을 피하고 이후 수정에 안전하지만, 큰 데이터프레임에서는 메모리를 추가로 사용합니다.
                missing_items = df_sm_latest_raw[missing_exp_date_filter].copy()
                st.subheader(f"미입력 ({len(missing_items)} 건)")
                if not missing_items.empty:
                    display_cols_missing = [RECEIPT_NUMBER_COL, PROD_CODE_COL, PROD_NAME_COL, RECEIPT_DATE_COL, BRANCH_COL]
                    missing_items_display = missing_items[[col for col in display_cols_missing if col in missing_items.columns]].copy()
                    if RECEIPT_DATE_COL in missing_items_display:
                        missing_items_display[RECEIPT_DATE_COL] = pd.to_datetime(missing_items_display[RECEIPT_DATE_COL]).dt.strftime('%Y-%m-%d').fillna('')
                    missing_items_display.rename(columns={RECEIPT_NUMBER_COL: '입고번호'}, inplace=True)
                    st.dataframe(missing_items_display, hide_index=True, use_container_width=True)
                else: 
                    st.success("✅ 누락 품목 없음")
            except KeyError as ke: 
                st.error(f"오류: 소비기한 누락 확인 중 필요한 컬럼({ke}) 없음")
            except Exception as e_filter: 
                st.error(f"소비기한 누락 필터링 오류: {e_filter}")

        with col2:
            st.header("⏳ 소비기한 임박 품목")
            try:
                if REMAINING_DAYS_COL not in df_sm_latest_raw.columns:
                    st.warning(f"'{REMAINING_DAYS_COL}' 컬럼이 없어 소비기한 임박 품목을 확인할 수 없습니다.")
                else:
                    # .copy() 사용: 원본 df_sm_latest_raw 변경 방지
                    df_check = df_sm_latest_raw.dropna(subset=[REMAINING_DAYS_COL]).copy()
                    df_check[REMAINING_DAYS_COL] = pd.to_numeric(df_check[REMAINING_DAYS_COL], errors='coerce')
                    df_check.dropna(subset=[REMAINING_DAYS_COL], inplace=True) 
                    
                    if not df_check.empty:
                        df_check[REMAINING_DAYS_COL] = df_check[REMAINING_DAYS_COL].astype(int)

                        cond1 = (df_check[PROD_NAME_COL].str.contains(KEYWORD_REFRIGERATED, na=False)) & \
                                (df_check[REMAINING_DAYS_COL] <= THRESHOLD_REFRIGERATED)
                        cond2 = (~df_check[PROD_NAME_COL].str.contains(KEYWORD_REFRIGERATED, na=False)) & \
                                (df_check[REMAINING_DAYS_COL] <= THRESHOLD_OTHER)
                        # .copy() 사용
                        imminent_items = df_check[cond1 | cond2].copy()

                        st.subheader(f"임박 ({len(imminent_items)} 건)")
                        st.markdown(f"- `{KEYWORD_REFRIGERATED}` 포함: **{THRESHOLD_REFRIGERATED}일 이하** / 나머지: **{THRESHOLD_OTHER}일 이하**")

                        if not imminent_items.empty:
                            display_cols_imminent = [PROD_CODE_COL, PROD_NAME_COL, BRANCH_COL, REMAINING_DAYS_COL, EXP_DATE_COL, QTY_COL, WGT_COL]
                            imminent_items_display = imminent_items[[col for col in display_cols_imminent if col in imminent_items.columns]].sort_values(by=REMAINING_DAYS_COL)
                            
                            def highlight_refrigerated_text_styler(val):
                                style = 'color: red; font-weight: bold;' if isinstance(val, str) and KEYWORD_REFRIGERATED in val else ''
                                return style

                            # FutureWarning 수정: Styler.applymap -> Styler.map
                            # .map()은 요소별로 함수를 적용합니다. highlight_refrigerated_text_styler는 이미 요소별 스타일을 반환합니다.
                            st.dataframe(
                                imminent_items_display.style.map(
                                    highlight_refrigerated_text_styler, subset=[PROD_NAME_COL]
                                ).format(
                                    {WGT_COL: "{:,.2f}", QTY_COL: "{:,.0f}"}
                                ),
                                hide_index=True, use_container_width=True
                            )
                        else:
                            st.success("✅ 소비기한 임박 품목 없음")
                    else:
                        st.info("잔여일수 데이터가 유효한 품목이 없어 소비기한 임박 품목을 확인할 수 없습니다.")

            except KeyError as ke: 
                st.error(f"오류: 소비기한 임박 확인 중 필요한 컬럼({ke}) 없음")
            except Exception as e_imminent: 
                st.error(f"소비기한 임박 필터링 오류: {e_imminent}")
        
        st.markdown("---")
        st.header("📦 장기 재고 현황 (입고 3개월 경과)")
        try:
            if RECEIPT_DATE_COL not in df_sm_latest_raw.columns:
                st.warning(f"'{RECEIPT_DATE_COL}' 컬럼이 없어 장기 재고 현황을 확인할 수 없습니다.")
            else:
                # .copy() 사용
                df_long_term_check = df_sm_latest_raw.copy()
                df_long_term_check = df_long_term_check[pd.notna(df_long_term_check[RECEIPT_DATE_COL])]

                if not df_long_term_check.empty:
                    # today_dt = datetime.date.today() # 이 방식은 Streamlit 캐싱과 함께 실행 시점에 따라 기준일이 달라질 수 있음
                    # 시트 이름 (YYYYMMDD)을 기준으로 오늘 날짜를 설정하는 것이 더 일관적일 수 있습니다.
                    # 여기서는 기존 로직을 유지하되, 매우 큰 시간차가 있는 과거 데이터 조회 시 인지 필요.
                    # 또는, latest_sheet_name을 datetime 객체로 변환하여 기준으로 삼는 것을 고려.
                    # 예: sheet_date = pd.to_datetime(latest_sheet_name, format='%Y%m%d').date()
                    # three_months_ago = sheet_date - relativedelta(months=3)
                    
                    today_dt = datetime.date.today() # 현재 날짜 기준
                    three_months_ago = today_dt - relativedelta(months=3)
                    
                    # .copy() 사용
                    long_term_items = df_long_term_check[
                        (df_long_term_check[RECEIPT_DATE_COL].dt.date < three_months_ago) & 
                        ((df_long_term_check[QTY_COL] > 0) | (df_long_term_check[WGT_COL] > 0))
                    ].copy()

                    st.subheader(f"3개월 이상 경과 재고 ({len(long_term_items)} 건)")
                    if not long_term_items.empty:
                        display_cols_long_term = [RECEIPT_NUMBER_COL, PROD_CODE_COL, PROD_NAME_COL, BRANCH_COL, RECEIPT_DATE_COL, 
                                                  QTY_COL, WGT_COL, INITIAL_QTY_BOX_COL, INITIAL_QTY_KG_COL] 
                        
                        long_term_items_display = long_term_items[[col for col in display_cols_long_term if col in long_term_items.columns]].sort_values(by=RECEIPT_DATE_COL)
                        
                        if RECEIPT_DATE_COL in long_term_items_display:
                            long_term_items_display[RECEIPT_DATE_COL] = pd.to_datetime(long_term_items_display[RECEIPT_DATE_COL]).dt.strftime('%Y-%m-%d').fillna('')
                        
                        long_term_items_display.rename(columns={
                            INITIAL_QTY_BOX_COL: '입고당시(Box)',
                            INITIAL_QTY_KG_COL: '입고당시(Kg)',
                            RECEIPT_NUMBER_COL: '입고번호' 
                        }, inplace=True)
                        
                        st.dataframe(
                            long_term_items_display.style.format({
                                WGT_COL: "{:,.2f}", 
                                QTY_COL: "{:,.0f}", 
                                '입고당시(Box)': "{:,.0f}",
                                '입고당시(Kg)': "{:,.2f}"
                            }),
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.success("✅ 입고 3개월 경과 재고 없음")
                else:
                    st.info("유효한 입고일자 데이터가 없어 장기 재고를 확인할 수 없습니다.")
        except KeyError as ke:
            st.error(f"오류: 장기 재고 확인 중 필요한 컬럼({ke}) 없음")
        except Exception as e_long_term:
            st.error(f"장기 재고 필터링 오류: {e_long_term}")
            # st.error(traceback.format_exc()) # 디버깅 시 상세 오류 출력

    else:
        st.error("SM 재고 데이터를 로드하지 못했거나 데이터가 비어있습니다. 파일 및 시트 내용을 확인해주세요.")
else:
    st.error(f"SM재고현황 파일 (ID: {SM_FILE_ID})에서 최신 날짜 시트를 찾을 수 없습니다.")

