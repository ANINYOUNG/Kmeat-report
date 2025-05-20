# pages/1_재고_비교_분석.py

import streamlit as st
import pandas as pd
import datetime
import os
import traceback
import numpy as np

# common_utils.py 에서 공통 파일 경로 및 유틸리티 함수 가져오기
from common_utils import ERP_FILE, SM_FILE, get_all_available_sheet_dates

# --- 이 페이지 고유의 설정 ---
LOCATION_MAP = {
    "냉동": "신갈냉동",
    "상이품/작업": "신갈상이품/작업",
    "선왕판매": "케이미트스토어"
}
ERP_TARGET_LOCATIONS = list(LOCATION_MAP.keys())
SM_TARGET_LOCATIONS = list(LOCATION_MAP.values())

SM_PROD_NAME_COL = '상품명' # SM 파일에서 사용할 상품명 컬럼 (processing.py 결과 기준)
SM_QTY_COL = '잔량(박스)'    # SM 파일에서 사용할 수량 컬럼
SM_WGT_COL = '잔량(Kg)'     # SM 파일에서 사용할 중량 컬럼


# --- 분석 함수 정의 (사용자 제공 원본 코드 유지) ---

@st.cache_data
def load_and_process_erp(filepath, sheet_name):
    erp_prod_name_col_raw = '품목명' 
    expected_cols = ['호실', '상품코드', '수량', '중량', erp_prod_name_col_raw]
    
    try:
        if not os.path.exists(filepath): st.error(f"오류: ERP 파일 '{os.path.basename(filepath)}' 없음"); return None
        df_erp_raw = pd.read_excel(filepath, sheet_name=sheet_name)
        st.info(f"ERP 원본 ({sheet_name}): {df_erp_raw.shape[0]} 행")

        if not all(col in df_erp_raw.columns for col in expected_cols):
            st.error(f"오류: ERP 시트({sheet_name}) 필요 컬럼({expected_cols}) 없음. 컬럼: {df_erp_raw.columns.tolist()}")
            return None

        df_erp = df_erp_raw[df_erp_raw['호실'].isin(ERP_TARGET_LOCATIONS)].copy()
        if df_erp.empty: st.warning(f"ERP 대상 호실({ERP_TARGET_LOCATIONS}) 데이터 없음 ({sheet_name})"); return pd.DataFrame()

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
        filtered_erp_count = original_erp_count - len(df_erp)
        if filtered_erp_count > 0: st.info(f"ERP: 수량/중량 0인 항목 {filtered_erp_count}건 제외")

        df_erp['key'] = df_erp['상품코드'] + '-' + df_erp['지점명']
        st.success(f"ERP 처리 완료 ({sheet_name}): {df_erp.shape[0]} 개 항목")
        return df_erp
    except FileNotFoundError: st.error(f"오류: ERP 파일 '{os.path.basename(filepath)}' 없음"); return None
    except ValueError as ve:
        if f"Worksheet named '{sheet_name}' not found" in str(ve): st.error(f"오류: ERP 파일에 '{sheet_name}' 시트 없음")
        else: st.error(f"ERP 데이터 로드/처리 중 값 오류: {ve}")
        return None
    except Exception as e: st.error(f"ERP 데이터 로드/처리 중 예상 못한 오류: {e}"); traceback.print_exc(); return None

@st.cache_data
def load_and_process_sm(filepath, sheet_name):
    try:
        if not os.path.exists(filepath): st.error(f"오류: SM 파일 '{os.path.basename(filepath)}' 없음"); return None
        required_sm_cols = ['지점명', '상품코드', SM_PROD_NAME_COL, SM_QTY_COL, SM_WGT_COL]
        df_sm_raw = pd.read_excel(filepath, sheet_name=sheet_name)
        st.info(f"SM 원본 ({sheet_name}): {df_sm_raw.shape[0]} 행")

        if not all(col in df_sm_raw.columns for col in required_sm_cols):
            missing_cols = [col for col in required_sm_cols if col not in df_sm_raw.columns]
            st.error(f"오류: SM 시트({sheet_name}) 필요 컬럼({missing_cols}) 없음. 컬럼: {df_sm_raw.columns.tolist()}")
            return None

        df_sm = df_sm_raw[df_sm_raw['지점명'].isin(SM_TARGET_LOCATIONS)].copy()
        if df_sm.empty: st.warning(f"SM 대상 지점명({SM_TARGET_LOCATIONS}) 데이터 없음 ({sheet_name})"); return pd.DataFrame()

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
        filtered_sm_count = original_sm_count - len(df_sm)
        if filtered_sm_count > 0: st.info(f"SM: {SM_QTY_COL}/{SM_WGT_COL} 0인 항목 {filtered_sm_count}건 제외")

        df_sm['key'] = df_sm['상품코드'] + '-' + df_sm['지점명']
        st.success(f"SM 처리 완료 ({sheet_name}): {df_sm.shape[0]} 개 항목.")
        return df_sm
    except FileNotFoundError: st.error(f"오류: SM 파일 '{os.path.basename(filepath)}' 없음"); return None
    except ValueError as ve:
        if f"Worksheet named '{sheet_name}' not found" in str(ve): st.error(f"오류: SM 파일에 '{sheet_name}' 시트 없음")
        else: st.error(f"SM 데이터 로드/처리 중 값 오류: {ve}")
        return None
    except Exception as e: st.error(f"SM 데이터 로드/처리 중 예상 못한 오류: {e}"); traceback.print_exc(); return None

def compare_inventories(df_erp, df_sm):
    if df_erp is None or df_sm is None or df_erp.empty or df_sm.empty : # 하나라도 비거나 None이면
        st.warning("ERP 또는 SM 데이터가 충분하지 않아 비교 분석을 수행할 수 없습니다.")
        erp_len = len(df_erp) if df_erp is not None else 0
        sm_len = len(df_sm) if df_sm is not None else 0
        
        summary = {'erp_total': erp_len, 'sm_total': sm_len, 'common_total': 0, 
                   'only_erp_count': erp_len, 'only_sm_count': sm_len,
                   'match_count': 0, 'mismatch_count': 0, 'match_rate': 0.0}
        
        # 컬럼 정의는 원본 코드 참조하여 유지
        only_erp_cols = ['상품코드', '상품명_ERP', '지점명', '수량', '중량'] # 상품명_ERP 사용
        only_sm_cols = ['상품코드', '상품명_SM', '지점명', SM_QTY_COL, SM_WGT_COL] # 상품명_SM 사용
        mismatch_cols = ['상품코드', '상품명', '지점명', '수량', SM_QTY_COL, '수량차이', '중량', SM_WGT_COL, '중량차이']

        df_only_erp = pd.DataFrame(columns=only_erp_cols)
        df_only_sm = pd.DataFrame(columns=only_sm_cols)
        if df_erp is not None and not df_erp.empty:
             # df_erp에는 '상품명_ERP' 컬럼이 있음. '상품명'은 없음.
            erp_display_cols = [col for col in only_erp_cols if col in df_erp.columns]
            df_only_erp = df_erp[erp_display_cols]


        if df_sm is not None and not df_sm.empty:
            # df_sm에는 '상품명_SM' 컬럼이 있음.
            sm_display_cols = [col for col in only_sm_cols if col in df_sm.columns]
            df_only_sm = df_sm[sm_display_cols]
            
        return summary, df_only_erp, df_only_sm, pd.DataFrame(columns=mismatch_cols)

    st.info("ERP-SM 데이터 병합 및 비교 중...")
    df_merged = pd.merge(
        df_erp[['key', '상품코드', '지점명', '상품명_ERP', '수량', '중량']],
        df_sm[['key', '상품명_SM', SM_QTY_COL, SM_WGT_COL]], # 상품명_SM 사용
        on='key', how='outer', indicator=True
    )

    num_cols = ['수량', '중량', SM_QTY_COL, SM_WGT_COL]
    for col in num_cols:
        if col in df_merged.columns: df_merged[col] = pd.to_numeric(df_merged[col].fillna(0), errors='coerce').fillna(0)
    
    df_merged['상품명_ERP'] = df_merged['상품명_ERP'].fillna('')
    df_merged['상품명_SM'] = df_merged['상품명_SM'].fillna('')
    # 최종 '상품명'은 ERP 우선, 없으면 SM 사용
    df_merged['상품명'] = df_merged.apply(lambda row: row['상품명_ERP'] if row['상품명_ERP'] else row['상품명_SM'], axis=1)

    only_erp = df_merged[df_merged['_merge'] == 'left_only'].copy()
    only_sm = df_merged[df_merged['_merge'] == 'right_only'].copy()
    both = df_merged[df_merged['_merge'] == 'both'].copy()
    
    # '상품명' 컬럼을 각 DataFrame에 명시적으로 다시 할당 (원본 코드에서는 key 분리 후 상품명 할당)
    if not only_erp.empty: only_erp['상품명'] = only_erp['상품명_ERP'] # ERP 전용은 ERP 상품명 사용
    if not only_sm.empty: 
        only_sm['상품명'] = only_sm['상품명_SM'] # SM 전용은 SM 상품명 사용
        # SM 전용 데이터 key 분리 (상품코드, 지점명 생성 위해)
        try:
            split_key = only_sm['key'].str.split('-', n=1, expand=True)
            only_sm['상품코드'] = split_key[0]
            only_sm['지점명'] = split_key[1]
        except Exception as e_split:
            st.warning(f"SM 전용 데이터 Key 분리 중 오류: {e_split}.")
            only_sm['상품코드'] = '분리 오류'
            only_sm['지점명'] = '분리 오류'


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
        erp_wgt_rounded = both['중량'].round(2) # ERP 중량 소수점 2자리 반올림
        sm_wgt_rounded = both[SM_WGT_COL].round(2) # SM 중량 소수점 2자리 반올림
        wgt_match = np.isclose(erp_wgt_rounded, sm_wgt_rounded, atol=tolerance)
        full_match = qty_match & wgt_match
        summary['match_count'] = int(full_match.sum())
        summary['mismatch_count'] = len(both) - summary['match_count']
        summary['match_rate'] = (summary['match_count'] / len(both)) * 100 if len(both) > 0 else 0.0
        
        mismatch_cols_def = ['상품코드', '상품명', '지점명', '수량', SM_QTY_COL, '수량차이', '중량', SM_WGT_COL, '중량차이']
        mismatches_list = both.loc[~full_match, [col for col in mismatch_cols_def if col in both.columns]].copy()


    only_erp_cols_def = ['상품코드', '상품명', '지점명', '수량', '중량']
    only_erp_return = only_erp[[col for col in only_erp_cols_def if col in only_erp.columns]] if not only_erp.empty else pd.DataFrame(columns=only_erp_cols_def)
    
    only_sm_cols_def = ['상품코드', '상품명', '지점명', SM_QTY_COL, SM_WGT_COL]
    only_sm_return = only_sm[[col for col in only_sm_cols_def if col in only_sm.columns]] if not only_sm.empty else pd.DataFrame(columns=only_sm_cols_def)

    if mismatches_list.empty: mismatches_list = pd.DataFrame(columns=mismatch_cols_def) # mismatch_cols_def 사용

    return summary, only_erp_return, only_sm_return, mismatches_list

# --- Streamlit 페이지 UI 구성 ---
# st.set_page_config(page_title="재고 비교 분석", layout="wide") # 메인 앱에서 한번만 호출 권장
st.title("🔄 ERP vs SM 재고 비교 분석") # 페이지 파일에서는 st.title 이나 st.header 사용
st.markdown("---")
st.markdown(f"대상 ERP 호실: `{', '.join(ERP_TARGET_LOCATIONS)}` ↔ 대상 SM 지점명: `{', '.join(SM_TARGET_LOCATIONS)}`")
st.markdown(f"SM 재고 비교 기준 컬럼: 수량=`{SM_QTY_COL}`, 중량=`{SM_WGT_COL}`")
st.markdown("---")

# --- 날짜 선택 UI 개선 ---
# SM_FILE을 기준으로 사용 가능한 날짜를 가져옵니다. ERP_FILE도 유사하게 가져와 공통 날짜를 찾을 수도 있습니다.
# 여기서는 SM_FILE 기준으로 사용자에게 날짜 선택 가이드를 제공합니다.
available_sm_dates = get_all_available_sheet_dates(SM_FILE) 

default_date_to_show = datetime.date.today()
min_date_for_picker = None
max_date_for_picker = None

if available_sm_dates:
    # 사용 가능한 날짜들(최신순 정렬됨)을 시간순으로 다시 정렬하여 min/max 설정
    available_sm_dates_asc = sorted(available_sm_dates, reverse=False)
    min_date_for_picker = available_sm_dates_asc[0]
    max_date_for_picker = available_sm_dates_asc[-1] # 이게 최신 날짜
    default_date_to_show = max_date_for_picker # 기본 선택을 사용 가능한 최신 날짜로
    
    st.info(f"SM 파일 기준 데이터 보유 날짜 범위: {min_date_for_picker.strftime('%Y-%m-%d')} ~ {max_date_for_picker.strftime('%Y-%m-%d')}")
    if st.checkbox("SM 파일 데이터 보유 모든 날짜 보기 (최신 100개)", False, key="cb_show_sm_dates_comparison"):
        display_limit = 100
        dates_to_show_str = [d.strftime('%Y-%m-%d') for d in available_sm_dates[:display_limit]] # 최신순
        st.markdown(f"<small>표시된 날짜 수: {len(dates_to_show_str)}. 전체 SM 데이터 보유 일수: {len(available_sm_dates)}</small>", unsafe_allow_html=True)
        st.text_area("SM 데이터 보유 날짜:", ", ".join(dates_to_show_str), height=100, key="sm_dates_list_area")
    st.markdown("<small>위 목록은 SM파일 기준이며, ERP파일에도 해당 날짜의 시트가 있어야 비교 가능합니다.</small>", unsafe_allow_html=True)
else:
    st.warning(f"'{os.path.basename(SM_FILE)}'에서 사용 가능한 날짜 정보를 찾을 수 없어 날짜 선택 범위를 제한할 수 없습니다. 수동으로 날짜를 선택해주세요.")

selected_date_obj = st.date_input(
    "분석 기준 날짜 선택", 
    default_date_to_show,
    min_value=min_date_for_picker, # 설정 가능하면 설정
    max_value=max_date_for_picker  # 설정 가능하면 설정
)

if selected_date_obj:
    target_sheet_name = selected_date_obj.strftime("%Y%m%d")
    st.info(f"**선택된 날짜:** {selected_date_obj.strftime('%Y-%m-%d')} (대상 시트: {target_sheet_name})")

    if st.button("재고 비교 분석 실행", key="btn_run_comparison"):
        st.info("분석 실행 전 ERP재고현황.xlsx 와 SM재고현황.xlsx 파일이 다른 프로그램에서 닫혀 있는지 확인해주세요.")
        with st.spinner("데이터를 로드하고 분석하는 중입니다..."):
            df_erp = load_and_process_erp(ERP_FILE, target_sheet_name)
            df_sm = load_and_process_sm(SM_FILE, target_sheet_name)

            # df_erp 또는 df_sm이 None(파일/시트 없음, 로드 오류) 또는 empty(대상 호실/지점 데이터 없음)일 수 있음
            # compare_inventories 함수 내에서 이미 None 또는 empty 케이스를 처리하고 있음

            summary, df_only_erp, df_only_sm, df_mismatches = compare_inventories(df_erp, df_sm)
            st.markdown("---")
            st.header("📊 분석 결과 요약")
            
            # 요약 정보 표시 (원본 코드와 동일하게 유지)
            col1, col2, col3 = st.columns(3); col4, col5, col6 = st.columns(3); col7, col8 = st.columns(2)
            col1.metric("ERP 대상 항목", summary['erp_total'])
            col2.metric("SM 대상 항목", summary['sm_total'])
            col3.metric("공통 항목", summary['common_total'])
            col4.metric("ERP 에만 존재", summary['only_erp_count'], delta=f"{summary['only_erp_count']}" if summary['only_erp_count'] else None, delta_color="off")
            col5.metric("SM 에만 존재", summary['only_sm_count'], delta=f"{summary['only_sm_count']}" if summary['only_sm_count'] else None, delta_color="off")
            col6.metric("완전 일치 항목", summary['match_count'])
            col7.metric("불일치 항목", summary['mismatch_count'], delta=f"{summary['mismatch_count']}" if summary['mismatch_count'] else None, delta_color="off")
            match_rate_display = f"{summary['match_rate']:.2f} %" if summary['common_total'] > 0 else "N/A" # 공통 항목 0일때 N/A
            col8.metric("🟢 재고 완전 일치율 (공통 항목 중)", match_rate_display) # 라벨 명확히
            st.markdown("---")

            st.header("📋 상세 분석 결과")
            if not df_only_erp.empty:
                with st.expander(f"ERP 에만 있는 항목 ({summary['only_erp_count']} 건)", expanded=False): # 기본적으로 닫힘
                    # '상품명_ERP'를 '상품명'으로 변경하여 표시 일관성 확보
                    df_only_erp_display = df_only_erp.rename(columns={'상품명_ERP': '상품명'})
                    st.dataframe(df_only_erp_display[['상품코드', '상품명', '지점명', '수량', '중량']], use_container_width=True) # 컬럼 순서 지정
            
            if not df_only_sm.empty:
                with st.expander(f"SM 에만 있는 항목 ({summary['only_sm_count']} 건)", expanded=False):
                    # '상품명_SM'을 '상품명'으로, SM_QTY_COL/SM_WGT_COL을 '수량'/'중량'과 유사하게 통일성 있게 표시
                    df_only_sm_display = df_only_sm.rename(columns={
                        '상품명_SM': '상품명', 
                        SM_QTY_COL: f'수량({SM_QTY_COL})', 
                        SM_WGT_COL: f'중량({SM_WGT_COL})'
                    })
                    display_cols_sm = ['상품코드', '상품명', '지점명', f'수량({SM_QTY_COL})', f'중량({SM_WGT_COL})']
                    st.dataframe(df_only_sm_display[display_cols_sm], use_container_width=True) # 컬럼 순서 지정

            if not df_mismatches.empty:
                with st.expander(f"수량/중량 불일치 항목 ({summary['mismatch_count']} 건)", expanded=True): # 불일치는 기본으로 열어둠
                    df_mismatches_display = df_mismatches.rename(columns={
                        '수량': '수량(ERP)', SM_QTY_COL: f'수량({SM_QTY_COL})', 
                        '중량': '중량(ERP)', SM_WGT_COL: f'중량({SM_WGT_COL})'
                    })
                    display_cols_mismatch = ['상품코드', '상품명', '지점명', '수량(ERP)', f'수량({SM_QTY_COL})', '수량차이', '중량(ERP)', f'중량({SM_WGT_COL})', '중량차이']
                    try: # 소수점 포맷팅 시도
                        for col_diff in ['수량차이', '중량차이']:
                            if col_diff in df_mismatches_display:
                                df_mismatches_display[col_diff] = pd.to_numeric(df_mismatches_display[col_diff], errors='coerce').map('{:,.2f}'.format)
                    except Exception as e_format:
                        print(f"차이값 포맷팅 중 오류: {e_format}") # 콘솔에만 오류 출력
                        
                    st.dataframe(df_mismatches_display[display_cols_mismatch], use_container_width=True) # 컬럼 순서 지정