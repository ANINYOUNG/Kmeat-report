# common_utils.py

import streamlit as st  # @st.cache_data 사용을 위해 필요
import pandas as pd     # 데이터 처리 함수 내에서 사용될 수 있음 (현재는 get_all_available_sheet_dates 에서는 직접 미사용)
import datetime
import os

# --- 1. 공통 파일 경로 및 기본 설정 ---
# 이 부분은 모든 페이지에서 동일하게 참조할 수 있는 경로 정보입니다.
DATA_FOLDER = r"C:\Users\kmeat 1f\Documents\googleaiy\list" # 사용자의 실제 경로로 수정하세요!
SM_FILE = os.path.join(DATA_FOLDER, 'SM재고현황.xlsx')
ERP_FILE = os.path.join(DATA_FOLDER, 'ERP재고현황.xlsx')


# --- 2. "창고별 재고 추이" 페이지 (inventory_app.py) 특화 설정 ---
# 만약 inventory_app.py (메인 페이지)가 이 설정들을 사용하고, 다른 페이지는 사용하지 않는다면,
# 여기에 두어 inventory_app.py 파일 자체를 간결하게 유지할 수 있습니다.
# 다른 페이지에서도 이 설정들이 필요하다면, 이름 규칙을 더 범용적으로 바꾸거나 구조를 고민해야 합니다.
# 현재는 "재고 추이" 페이지용으로 명시합니다.
REPORT_LOCATION_MAP_TREND = {
    "신갈냉동": "냉동",
    "선왕CH4층": "선왕",
    "신갈김형제": "김형제",
    "신갈상이품/작업": "상이품"
}
TARGET_SM_LOCATIONS_FOR_TREND = list(REPORT_LOCATION_MAP_TREND.keys())
SM_QTY_COL_TREND = '잔량(박스)' # "재고 추이" 페이지에서 SM파일의 수량 컬럼명
SM_WGT_COL_TREND = '잔량(Kg)'  # "재고 추이" 페이지에서 SM파일의 중량 컬럼명
REPORT_ROW_ORDER_TREND = ["냉동", "선왕", "상이품", "김형제"] # "재고 추이" 페이지의 표 행 순서


# --- 3. 공통 유틸리티 함수 ---

@st.cache_data # 파일 I/O 결과를 캐싱하여 성능 향상
def get_all_available_sheet_dates(filepath):
    """
    지정된 엑셀 파일에서 'YYYYMMDD' 형식의 시트 이름을 찾아
    datetime.date 객체 리스트로 반환합니다. 리스트는 최신 날짜 순으로 정렬됩니다.
    파일이 없거나 읽기 오류 시 빈 리스트를 반환하고 콘솔에 메시지를 출력합니다.
    """
    available_dates = []
    if not os.path.exists(filepath):
        # Streamlit UI 요소(예: st.error)는 여기서 직접 사용하기보다는,
        # 이 함수를 호출한 페이지에서 UI 피드백을 주는 것이 좋습니다.
        # 여기서는 print로 콘솔에 경고를 남깁니다.
        print(f"경고: '{os.path.basename(filepath)}' 파일을 찾을 수 없습니다. (get_all_available_sheet_dates)")
        return available_dates
    try:
        with pd.ExcelFile(filepath) as xls:
            sheet_names = xls.sheet_names
        for sheet_name in sheet_names:
            try:
                # 시트 이름이 정확히 'YYYYMMDD' 형식인지 확인
                dt_object = datetime.datetime.strptime(sheet_name, "%Y%m%d").date()
                available_dates.append(dt_object)
            except ValueError:
                # 날짜 형식이 아닌 시트 이름은 조용히 무시
                pass
        available_dates.sort(reverse=True) # 최신 날짜가 맨 앞으로 오도록 정렬
        return available_dates
    except Exception as e:
        print(f"오류: '{os.path.basename(filepath)}' 파일의 시트 목록을 읽는 중 오류 발생: {e} (get_all_available_sheet_dates)")
        return [] # 오류 발생 시 빈 리스트 반환

# 만약 여러 페이지에서 'SM재고현황.xlsx' 파일의 특정 날짜 데이터를 로드하는
# 매우 범용적인 로더가 필요하다면 여기에 추가할 수 있습니다.
# 예: def load_generic_sm_sheet(filepath, date_str_yyyymmdd, 필요한컬럼리스트) -> pd.DataFrame:
# 하지만 현재 "창고별 재고 추이"와 "재고 비교 분석"은 각자의 특화된 로딩 및 전처리 함수를 가지고 있으므로,
# 해당 함수들은 각 페이지 파일에 두는 것이 더 적절해 보입니다.
# load_sm_data_for_selected_dates 함수는 "창고별 재고 추이" 페이지에 특화된 컬럼명을 사용하므로,
# inventory_app.py 파일에 있거나, 여기서 가져다 쓴다면 이름에 _trend를 붙이는 등 명확히 구분하는 것이 좋습니다.
# 지금은 inventory_app.py 내에 해당 로직이 포함되어 있으므로, common_utils.py에서는 생략합니다.