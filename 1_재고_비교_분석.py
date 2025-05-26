# pages/1_재고_비교_분석.py

import streamlit as st
import pandas as pd
import datetime
# import os # os.path.exists는 더 이상 직접 사용하지 않음
import traceback
import numpy as np

# common_utils.py 에서 공통 유틸리티 함수 및 상수 가져오기
from common_utils import (
    download_excel_from_drive_as_bytes, 
    get_all_available_sheet_dates_from_bytes,
    SM_QTY_COL_TREND as SM_QTY_COL, 
    SM_WGT_COL_TREND as SM_WGT_COL
)

# --- Google Drive 파일 ID 정의 ---
# 사용자님이 제공해주신 실제 파일 ID를 사용합니다.
ERP_FILE_ID = "1Lbtwenw8LcDaj94_J4kKTjoWQY7PEAZs"
SM_FILE_ID = "1tRljdvOpp4fITaVEXvoL9mNveNg2qt4p"
# --- 파일 ID 정의 끝 ---


# --- 이 페이지 고유의 설정 ---
LOCATION_MAP = {
    "냉동": "신갈냉동",
    "상이품/작업": "신갈상이품/작업",
    "선왕판매": "배정분"  # "케이미트스토어"를 "배정분"으로 수정
}
ERP_TARGET_LOCATIONS = list(LOCATION_MAP.keys())
SM_TARGET_LOCATIONS = list(LOCATION_MAP.values())

SM_PROD_NAME_COL = '상품명' 
# SM_QTY_COL 와 SM_WGT_COL 은 common_utils에서 가져온 것을 사용


# --- Google Drive 서비스 객체 가져오기 ---
retrieved_drive_service = st.session_state.get('drive_service')
page_title_for_debug = "재고 비교 분석 페이지" 

if retrieved_drive_service:
    st.sidebar.info(f"'{page_title_for_debug}'에서 Drive Service 로드 성공!")
else:
    st.sidebar.error(f"'{page_title_for_debug}'에서 Drive Service 로드 실패! (None). 메인 페이지를 먼저 방문하여 인증을 완료해주세요.")

drive_service = retrieved_drive_service


# --- 분석 함수 정의 (Google Drive 연동으로 수정) ---
@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None}) # drive_service 해시 방지
def load_and_process_erp(_drive_service, file_id_erp, sheet_name): 
    erp_prod_name_col_raw = '품목명' 
    expected_cols = ['호실', '상품코드', '수량', '중량', erp_prod_name_col_raw]
    
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (ERP 데이터 로딩)")
        return None

    file_bytes_erp = download_excel_from_drive_as_bytes(_drive_service, file_id_erp, f"ERP 재고현황 ({sheet_name})")
    if file_bytes_erp is None:
        return None
        
    try:
        df_erp_raw = pd.read_excel(file_bytes_erp, sheet_name=sheet_name)
        # st.info(f"ERP 원본 ({sheet_name}): {df_erp_raw.shape[0]} 행")

        if not all(col in df_erp_raw.columns for col in expected_cols):
            st.error(f"오류: ERP 시트({sheet_name}) 필요 컬럼({expected_cols}) 없음. 컬럼: {df_erp_raw.columns.tolist()}")
            return None

        df_erp = df_erp_raw[df_erp_raw['호실'].isin(ERP_TARGET_LOCATIONS)].copy()
        if df_erp.empty: 
            st.warning(f"ERP 대상 호실({ERP_TARGET_LOCATIONS}) 데이터 없음 ({sheet_name})")
            return pd.DataFrame()

        df_erp = df_erp[['호실', '상품코드', erp_prod_name_col_raw, '수량', '중량']].copy()
        df_erp['지점명'] = df_erp['호실'].map(LOCATION_MAP)
        df_erp.drop(columns=['호실'], inplace=True)
        df_erp['상품코드'] = df_erp['상품코드'].astype(str).str.strip()
        df_erp[erp_prod_name_col_raw] = df_erp[erp_prod_name_col_raw].astype(str).str.strip()
        df_erp['수량'] = pd.to_numeric(df_erp['수량'], errors='coerce').fillna(0)
        df_erp['중량'] = pd.to_numeric(df_erp['중량'], errors='coerce').fillna(0)

        if not df_erp.empty:
            df_erp = df_erp.groupby(['지점명', '상품코드'], as_index=False).agg(
                상품명_ERP=(erp_prod_name_col_raw, 'first'),
                수량=('수량', 'sum'),
                중량=('중량', 'sum')
            )
        
        original_erp_count = len(df_erp)
        if not df_erp.empty: df_erp = df_erp[~((df_erp['수량'] == 0) & (df_erp['중량'] == 0))]
        
        df_erp['key'] = df_erp['상품코드'] + '-' + df_erp['지점명']
        return df_erp
    except ValueError as ve:
        if f"Worksheet named '{sheet_name}' not found" in str(ve): 
            st.error(f"오류: ERP 파일 (ID: {file_id_erp})에 '{sheet_name}' 시트 없음")
        else: 
            st.error(f"ERP 데이터 (ID: {file_id_erp}, 시트: {sheet_name}) 로드/처리 중 값 오류: {ve}")
        return None
    except Exception as e: 
        st.error(f"ERP 데이터 (ID: {file_id_erp}, 시트: {sheet_name}) 로드/처리 중 예상 못한 오류: {e}")
        return None

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None}) # drive_service 해시 방지
def load_and_process_sm(_drive_service, file_id_sm, sheet_name): 
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (SM 데이터 로딩)")
        return None

    file_bytes_sm = download_excel_from_drive_as_bytes(_drive_service, file_id_sm, f"SM 재고현황 ({sheet_name})")
    if file_bytes_sm is None:
        return None

    try:
        required_sm_cols = ['지점명', '상품코드', SM_PROD_NAME_COL, SM_QTY_COL, SM_WGT_COL]
        df_sm_raw = pd.read_excel(file_bytes_sm, sheet_name=sheet_name)
        # st.info(f"SM 원본 ({sheet_name}): {df_sm_raw.shape[0]} 행")

        if not all(col in df_sm_raw.columns for col in required_sm_cols):
            missing_cols = [col for col in required_sm_cols if col not in df_sm_raw.columns]
            st.error(f"오류: SM 시트({sheet_name}) 필요 컬럼({missing_cols}) 없음. 컬럼: {df_sm_raw.columns.tolist()}")
            return None

        df_sm = df_sm_raw[df_sm_raw['지점명'].isin(SM_TARGET_LOCATIONS)].copy()
        if df_sm.empty: 
            st.warning(f"SM 대상 지점명({SM_TARGET_LOCATIONS}) 데이터 없음 ({sheet_name})")
            return pd.DataFrame()

        df_sm = df_sm[required_sm_cols].copy()
        df_sm['상품코드'] = df_sm['상품코드'].astype(str).str.strip()
        df_sm['지점명'] = df_sm['지점명'].astype(str).str.strip()
        df_sm[SM_PROD_NAME_COL] = df_sm[SM_PROD_NAME_COL].astype(str).str.strip()
        df_sm[SM_QTY_COL] = pd.to_numeric(df_sm[SM_QTY_COL], errors='coerce').fillna(0)
        df_sm[SM_WGT_COL] = pd.to_numeric(df_sm[SM_WGT_COL], errors='coerce').fillna(0)

        if not df_sm.empty:
            df_sm = df_sm.groupby(['지점명', '상품코드'], as_index=False).agg(
                상품명_SM=(SM_PROD_NAME_COL, 'first'),
                QtySum=(SM_QTY_COL, 'sum'),
                WgtSum=(SM_WGT_COL, 'sum')
            ).rename(columns={'QtySum': SM_QTY_COL, 'WgtSum': SM_WGT_COL})

        original_sm_count = len(df_sm)
        if not df_sm.empty: df_sm = df_sm[~((df_sm[SM_QTY_COL] == 0) & (df_sm[SM_WGT_COL] == 0))]
        
        df_sm['key'] = df_sm['상품코드'] + '-' + df_sm['지점명']
        return df_sm
    except ValueError as ve:
        if f"Worksheet named '{sheet_name}' not found" in str(ve): 
            st.error(f"오류: SM 파일 (ID: {file_id_sm})에 '{sheet_name}' 시트 없음")
        else: 
            st.error(f"SM 데이터 (ID: {file_id_sm}, 시트: {sheet_name}) 로드/처리 중 값 오류: {ve}")
        return None
    except Exception as e: 
        st.error(f"SM 데이터 (ID: {file_id_sm}, 시트: {sheet_name}) 로드/처리 중 예상 못한 오류: {e}")
        return None

def compare_inventories(df_erp, df_sm):
    if df_erp is None or df_sm is None or df_erp.empty or df_sm.empty : 
        st.warning("ERP 또는 SM 데이터가 충분하지 않아 비교 분석을 수행할 수 없습니다.")
        erp_len = len(df_erp) if df_erp is not None and not df_erp.empty else 0
        sm_len = len(df_sm) if df_sm is not None and not df_sm.empty else 0
        
        summary = {'erp_total': erp_len, 'sm_total': sm_len, 'common_total': 0, 
                   'only_erp_count': erp_len, 'only_sm_count': sm_len,
                   'match_count': 0, 'mismatch_count': 0, 'match_rate': 0.0}
        
        only_erp_cols = ['상품코드', '상품명_ERP', '지점명', '수량', '중량'] 
        only_sm_cols = ['상품코드', '상품명_SM', '지점명', SM_QTY_COL, SM_WGT_COL] 
        mismatch_cols = ['상품코드', '상품명', '지점명', '수량', SM_QTY_COL, '수량차이', '중량', SM_WGT_COL, '중량차이']

        df_only_erp = pd.DataFrame(columns=only_erp_cols)
        df_only_sm = pd.DataFrame(columns=only_sm_cols)

        if df_erp is not None and not df_erp.empty:
            erp_display_cols = [col for col in only_erp_cols if col in df_erp.columns]
            df_only_erp = df_erp[erp_display_cols].copy()

        if df_sm is not None and not df_sm.empty:
            sm_display_cols = [col for col in only_sm_cols if col in df_sm.columns]
            df_only_sm = df_sm[sm_display_cols].copy()
            
        return summary, df_only_erp, df_only_sm, pd.DataFrame(columns=mismatch_cols)

    df_merged = pd.merge(
        df_erp[['key', '상품코드', '지점명', '상품명_ERP', '수량', '중량']],
        df_sm[['key', '상품명_SM', SM_QTY_COL, SM_WGT_COL]], 
        on='key', how='outer', indicator=True
    )

    num_cols = ['수량', '중량', SM_QTY_COL, SM_WGT_COL]
    for col in num_cols:
        if col in df_merged.columns: df_merged[col] = pd.to_numeric(df_merged[col].fillna(0), errors='coerce').fillna(0)
    
    df_merged['상품명_ERP'] = df_merged['상품명_ERP'].fillna('')
    df_merged['상품명_SM'] = df_merged['상품명_SM'].fillna('')
    df_merged['상품명'] = df_merged.apply(lambda row: row['상품명_ERP'] if row['상품명_ERP'] else row['상품명_SM'], axis=1)

    only_erp = df_merged[df_merged['_merge'] == 'left_only'].copy()
    only_sm = df_merged[df_merged['_merge'] == 'right_only'].copy()
    both = df_merged[df_merged['_merge'] == 'both'].copy()
    
    if not only_erp.empty: only_erp['상품명'] = only_erp['상품명_ERP'] 
    if not only_sm.empty: 
        only_sm['상품명'] = only_sm['상품명_SM'] 
        try:
            split_key = only_sm['key'].str.split('-', n=1, expand=True)
            if split_key.shape[1] > 0: only_sm['상품코드'] = split_key[0]
            if split_key.shape[1] > 1: only_sm['지점명'] = split_key[1]
        except Exception as e_split:
            st.warning(f"SM 전용 데이터 Key 분리 중 오류: {e_split}.")
            if '상품코드' not in only_sm.columns: only_sm['상품코드'] = '분리 오류'
            if '지점명' not in only_sm.columns: only_sm['지점명'] = '분리 오류'

    summary = {
        'erp_total': len(df_erp), 'sm_total': len(df_sm), 'common_total': len(both),
        'only_erp_count': len(only_erp), 'only_sm_count': len(only_sm),
        'match_count': 0, 'mismatch_count': 0, 'match_rate': 0.0
    }
    mismatches_list = pd.DataFrame()

    if not both.empty:
        both['수량차이'] = both['수량'] - both[SM_QTY_COL]
        both['중량차이'] = both['중량'] - both[SM_WGT_COL]
        tolerance = 1e-9
        qty_match = np.isclose(both['수량차이'], 0, atol=tolerance)
        erp_wgt_rounded = both['중량'].round(2) 
        sm_wgt_rounded = both[SM_WGT_COL].round(2) 
        wgt_match = np.isclose(erp_wgt_rounded, sm_wgt_rounded, atol=tolerance)
        full_match = qty_match & wgt_match
        summary['match_count'] = int(full_match.sum())
        summary['mismatch_count'] = len(both) - summary['match_count']
        summary['match_rate'] = (summary['match_count'] / len(both)) * 100 if len(both) > 0 else 0.0
        
        mismatch_cols_def = ['상품코드', '상품명', '지점명', '수량', SM_QTY_COL, '수량차이', '중량', SM_WGT_COL, '중량차이']
        mismatches_list = both.loc[~full_match, [col for col in mismatch_cols_def if col in both.columns]].copy()

    only_erp_cols_def = ['상품코드', '상품명', '지점명', '수량', '중량']
    only_erp_return = only_erp[[col for col in only_erp_cols_def if col in only_erp.columns]].copy() if not only_erp.empty else pd.DataFrame(columns=only_erp_cols_def)
    
    only_sm_cols_def = ['상품코드', '상품명', '지점명', SM_QTY_COL, SM_WGT_COL]
    only_sm_return = only_sm[[col for col in only_sm_cols_def if col in only_sm.columns]].copy() if not only_sm.empty else pd.DataFrame(columns=only_sm_cols_def)

    if mismatches_list.empty: mismatches_list = pd.DataFrame(columns=mismatch_cols_def)

    return summary, only_erp_return, only_sm_return, mismatches_list

# --- Streamlit 페이지 UI 구성 ---
st.title("🔄 ERP vs SM 재고 비교 분석")
st.markdown("---")

if drive_service is None: 
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()

st.markdown(f"대상 ERP 호실: `{', '.join(ERP_TARGET_LOCATIONS)}` ↔ 대상 SM 지점명: `{', '.join(SM_TARGET_LOCATIONS)}`")
st.markdown(f"SM 재고 비교 기준 컬럼: 수량=`{SM_QTY_COL}`, 중량=`{SM_WGT_COL}`")
st.markdown("---")

available_sm_dates = []
# SM_FILE_ID가 플레이스홀더가 아닌 실제 ID인지 확인
if SM_FILE_ID and not SM_FILE_ID.startswith("YOUR_"): 
    sm_file_bytes = download_excel_from_drive_as_bytes(drive_service, SM_FILE_ID, "SM재고현황 (날짜조회용)")
    if sm_file_bytes:
        available_sm_dates = get_all_available_sheet_dates_from_bytes(sm_file_bytes, "SM재고현황 (날짜조회용)")
else:
    st.warning("SM_FILE_ID가 코드에 올바르게 설정되지 않았습니다. 코드 상단에서 실제 파일 ID로 수정해주세요.")


default_date_to_show = datetime.date.today()
min_date_for_picker = None
max_date_for_picker = None

if available_sm_dates:
    available_sm_dates_asc = sorted(available_sm_dates, reverse=False)
    min_date_for_picker = available_sm_dates_asc[0]
    max_date_for_picker = available_sm_dates_asc[-1] 
    default_date_to_show = max_date_for_picker 
    
    st.info(f"SM 파일 기준 데이터 보유 날짜 범위: {min_date_for_picker.strftime('%Y-%m-%d')} ~ {max_date_for_picker.strftime('%Y-%m-%d')}")
    if st.checkbox("SM 파일 데이터 보유 모든 날짜 보기 (최신 100개)", False, key="cb_show_sm_dates_comparison"):
        display_limit = 100
        dates_to_show_str = [d.strftime('%Y-%m-%d') for d in sorted(available_sm_dates, reverse=True)[:display_limit]]
        st.markdown(f"<small>표시된 날짜 수: {len(dates_to_show_str)}. 전체 SM 데이터 보유 일수: {len(available_sm_dates)}</small>", unsafe_allow_html=True)
        st.text_area("SM 데이터 보유 날짜:", ", ".join(dates_to_show_str), height=100, key="sm_dates_list_area")
    st.markdown("<small>위 목록은 SM파일 기준이며, ERP파일에도 해당 날짜의 시트가 있어야 비교 가능합니다.</small>", unsafe_allow_html=True)
else:
    # SM_FILE_ID가 설정되었지만 날짜 정보를 못 가져온 경우에 대한 경고
    if SM_FILE_ID and not SM_FILE_ID.startswith("YOUR_"): 
        st.warning(f"'SM재고현황.xlsx' (ID: {SM_FILE_ID})에서 사용 가능한 날짜 정보를 찾을 수 없어 날짜 선택 범위를 제한할 수 없습니다. 수동으로 날짜를 선택해주세요.")

selected_date_obj = st.date_input(
    "분석 기준 날짜 선택", 
    default_date_to_show,
    min_value=min_date_for_picker,
    max_value=max_date_for_picker
)

if selected_date_obj:
    target_sheet_name = selected_date_obj.strftime("%Y%m%d")
    st.info(f"**선택된 날짜:** {selected_date_obj.strftime('%Y-%m-%d')} (대상 시트: {target_sheet_name})")

    if st.button("재고 비교 분석 실행", key="btn_run_comparison"):
        # ERP_FILE_ID와 SM_FILE_ID가 플레이스홀더가 아닌지 다시 한번 확인
        if (ERP_FILE_ID and ERP_FILE_ID.startswith("YOUR_")) or \
           (SM_FILE_ID and SM_FILE_ID.startswith("YOUR_")):
            st.error("ERP 또는 SM 파일 ID가 코드에 올바르게 설정되지 않았습니다. 코드 상단의 파일 ID를 확인해주세요.")
        else:
            with st.spinner("데이터를 로드하고 분석하는 중입니다..."):
                df_erp = load_and_process_erp(drive_service, ERP_FILE_ID, target_sheet_name)
                df_sm = load_and_process_sm(drive_service, SM_FILE_ID, target_sheet_name)

                summary, df_only_erp, df_only_sm, df_mismatches = compare_inventories(df_erp, df_sm)
                
                st.markdown("---")
                st.header("📊 분석 결과 요약")
                
                col1, col2, col3 = st.columns(3); col4, col5, col6 = st.columns(3); col7, col8 = st.columns(2)
                col1.metric("ERP 대상 항목", summary['erp_total'])
                col2.metric("SM 대상 항목", summary['sm_total'])
                col3.metric("공통 항목", summary['common_total'])
                col4.metric("ERP 에만 존재", summary['only_erp_count'], delta=f"{summary['only_erp_count']}" if summary['only_erp_count'] else None, delta_color="off")
                col5.metric("SM 에만 존재", summary['only_sm_count'], delta=f"{summary['only_sm_count']}" if summary['only_sm_count'] else None, delta_color="off")
                col6.metric("완전 일치 항목", summary['match_count'])
                col7.metric("불일치 항목", summary['mismatch_count'], delta=f"{summary['mismatch_count']}" if summary['mismatch_count'] else None, delta_color="off")
                match_rate_display = f"{summary['match_rate']:.2f} %" if summary['common_total'] > 0 else "N/A"
                col8.metric("🟢 재고 완전 일치율 (공통 항목 중)", match_rate_display)
                st.markdown("---")

                st.header("📋 상세 분석 결과")
                if df_only_erp is not None and not df_only_erp.empty: 
                    with st.expander(f"ERP 에만 있는 항목 ({summary['only_erp_count']} 건)", expanded=False):
                        df_only_erp_display = df_only_erp.rename(columns={'상품명_ERP': '상품명'})
                        st.dataframe(df_only_erp_display[['상품코드', '상품명', '지점명', '수량', '중량']], use_container_width=True)
                
                if df_only_sm is not None and not df_only_sm.empty: 
                    with st.expander(f"SM 에만 있는 항목 ({summary['only_sm_count']} 건)", expanded=False):
                        df_only_sm_display = df_only_sm.rename(columns={
                            '상품명_SM': '상품명', 
                            SM_QTY_COL: f'수량({SM_QTY_COL.replace("잔량(","").replace(")","")})', 
                            SM_WGT_COL: f'중량({SM_WGT_COL.replace("잔량(","").replace(")","")})'
                        })
                        display_cols_sm = ['상품코드', '상품명', '지점명', 
                                           f'수량({SM_QTY_COL.replace("잔량(","").replace(")","")})', 
                                           f'중량({SM_WGT_COL.replace("잔량(","").replace(")","")})']
                        st.dataframe(df_only_sm_display[[col for col in display_cols_sm if col in df_only_sm_display.columns]], use_container_width=True)

                if df_mismatches is not None and not df_mismatches.empty: 
                    with st.expander(f"수량/중량 불일치 항목 ({summary['mismatch_count']} 건)", expanded=True):
                        df_mismatches_display = df_mismatches.rename(columns={
                            '수량': '수량(ERP)', SM_QTY_COL: f'수량(SM)', 
                            '중량': '중량(ERP)', SM_WGT_COL: f'중량(SM)'
                        })
                        display_cols_mismatch = ['상품코드', '상품명', '지점명', '수량(ERP)', f'수량(SM)', '수량차이', '중량(ERP)', f'중량(SM)', '중량차이']
                        try:
                            for col_diff in ['수량차이', '중량차이']:
                                if col_diff in df_mismatches_display:
                                    df_mismatches_display[col_diff] = pd.to_numeric(df_mismatches_display[col_diff], errors='coerce').map('{:,.2f}'.format)
                        except Exception as e_format:
                            st.caption(f"차이값 포맷팅 중 작은 오류 발생: {e_format}")
                        st.dataframe(df_mismatches_display[[col for col in display_cols_mismatch if col in df_mismatches_display.columns]], use_container_width=True)
else:
    st.info("분석할 날짜를 선택해주세요.")