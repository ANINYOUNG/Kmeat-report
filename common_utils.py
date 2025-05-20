# common_utils.py

import streamlit as st
import pandas as pd
import datetime
import io # io.BytesIO 사용을 위해 추가
# googleapiclient.http는 download_excel_from_drive 함수 내에서 사용되므로,
# 해당 함수가 이 파일로 옮겨지거나, 이 파일에서 호출된다면 필요합니다.
# 지금은 get_all_available_sheet_dates 함수만 수정하므로 직접적인 import는 필요 없을 수 있습니다.
# 하지만 데이터 로딩 관련 유틸리티 파일이므로, 관련 import를 미리 포함하는 것도 좋습니다.
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# --- 1. 공통 Google Drive 파일 ID 정의 (예시) ---
# 실제 파일 ID는 메인 앱이나 각 페이지에서 불러와서 함수에 전달하는 것이 더 유연할 수 있습니다.
# 또는, 앱 전체에서 고정적으로 사용되는 파일 ID라면 여기에 정의할 수 있습니다.
# 예시: SM_FILE_ID = "YOUR_ACTUAL_SM_FILE_ID_HERE"
# 예시: ERP_FILE_ID = "YOUR_ACTUAL_ERP_FILE_ID_HERE"

# --- 2. "창고별 재고 추이" 페이지 (inventory_app.py) 특화 설정 ---
# 이 상수들이 여러 페이지에서 공통으로 사용된다면 여기에 두어도 좋습니다.
# 특정 페이지에서만 사용된다면 해당 페이지로 옮기는 것을 고려할 수 있습니다.
REPORT_LOCATION_MAP_TREND = {
    "신갈냉동": "냉동",
    "선왕CH4층": "선왕",
    "신갈김형제": "김형제",
    "신갈상이품/작업": "상이품"
}
TARGET_SM_LOCATIONS_FOR_TREND = list(REPORT_LOCATION_MAP_TREND.keys())
SM_QTY_COL_TREND = '잔량(박스)'
SM_WGT_COL_TREND = '잔량(Kg)'
REPORT_ROW_ORDER_TREND = ["냉동", "선왕", "상이품", "김형제"]


# --- 3. 공통 유틸리티 함수 ---

@st.cache_data(ttl=300) # API 호출 및 데이터 처리 결과를 캐싱하여 성능 향상
def download_excel_from_drive_as_bytes(drive_service, file_id, file_name_for_error_msg="Excel file"):
    """
    Google Drive에서 특정 파일 ID의 엑셀 파일을 다운로드하여
    io.BytesIO 객체로 반환합니다.
    오류 발생 시 None을 반환하고 Streamlit UI에 오류 메시지를 표시합니다.
    """
    if drive_service is None:
        st.error(f"오류: Google Drive 서비스가 초기화되지 않았습니다. ({file_name_for_error_msg} 다운로드 시도)")
        return None
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        # st.success(f"'{file_name_for_error_msg}' 파일을 성공적으로 다운로드했습니다.") # 성공 메시지는 호출하는 쪽에서 필요시 표시
        return fh
    except HttpError as error:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 다운로드 실패: {error.resp.status} - {error._get_reason()}. 파일 공유 설정을 확인하세요.")
        return None
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 처리 중 예외 발생: {e}")
        return None

@st.cache_data(ttl=300) # 파일 내용 기반 캐싱
def get_all_available_sheet_dates_from_bytes(file_content_bytes, file_name_for_error_msg="Excel file"):
    """
    io.BytesIO 객체 (엑셀 파일 내용)에서 'YYYYMMDD' 형식의 시트 이름을 찾아
    datetime.date 객체 리스트로 반환합니다. 리스트는 최신 날짜 순으로 정렬됩니다.
    파일 내용이 없거나 읽기 오류 시 빈 리스트를 반환하고 UI에 경고를 표시합니다.
    """
    available_dates = []
    if file_content_bytes is None:
        st.warning(f"경고: '{file_name_for_error_msg}' 파일 내용이 없어 시트 날짜를 추출할 수 없습니다.")
        return available_dates
    try:
        with pd.ExcelFile(file_content_bytes) as xls:
            sheet_names = xls.sheet_names
        for sheet_name in sheet_names:
            try:
                dt_object = datetime.datetime.strptime(sheet_name, "%Y%m%d").date()
                available_dates.append(dt_object)
            except ValueError:
                # 날짜 형식이 아닌 시트 이름은 조용히 무시
                pass
        available_dates.sort(reverse=True) # 최신 날짜가 맨 앞으로 오도록 정렬
        return available_dates
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' 파일의 시트 목록을 읽는 중 오류 발생: {e}")
        return []

# --- 데이터 로딩 함수 예시 (필요시 각 페이지 또는 여기에 추가) ---
# 예: SM재고현황 파일의 특정 날짜 시트를 읽어오는 함수
@st.cache_data(ttl=300)
def load_sm_sheet_data(drive_service, file_id, date_str_yyyymmdd, file_name_for_error_msg="SM재고현황"):
    """
    지정된 Google Drive 파일 ID에서 특정 날짜(YYYYMMDD)의 시트 데이터를 DataFrame으로 로드합니다.
    """
    file_bytes = download_excel_from_drive_as_bytes(drive_service, file_id, file_name_for_error_msg)
    if file_bytes is None:
        return None # 파일 다운로드 실패

    try:
        # 특정 시트를 읽기 전에 해당 시트가 존재하는지 확인하는 로직이 추가될 수 있습니다.
        # 여기서는 pd.read_excel이 시트 이름으로 바로 읽기를 시도합니다.
        df_sheet = pd.read_excel(file_bytes, sheet_name=date_str_yyyymmdd, header=0)
        df_sheet.dropna(how='all', inplace=True)
        if df_sheet.empty:
            # st.info(f"'{file_name_for_error_msg}' 파일의 '{date_str_yyyymmdd}' 시트에 데이터가 없습니다.")
            return pd.DataFrame() # 빈 데이터프레임 반환

        # --- 이 부분부터는 'SM재고현황' 파일의 특정 구조에 대한 처리입니다. ---
        # --- 만약 다른 종류의 엑셀 파일을 읽는다면 이 부분은 달라져야 합니다. ---
        required_cols = ['지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND] # SM재고현황 파일용 상수 사용
        if not all(col in df_sheet.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_sheet.columns]
            st.warning(f"경고: '{file_name_for_error_msg}' 파일의 '{date_str_yyyymmdd}' 시트에 필수 컬럼 {missing} 중 일부가 누락되었습니다.")
            # 상황에 따라 빈 DataFrame을 반환하거나, 부분 데이터라도 처리할지 결정
            return pd.DataFrame() 

        df_sheet_copy = df_sheet.copy()
        df_sheet_copy['날짜'] = pd.to_datetime(date_str_yyyymmdd, format='%Y%m%d')
        
        # 필요한 컬럼만 선택하고, 숫자형으로 변환 (SM재고현황 파일에 특화된 컬럼명 사용)
        df_processed_sheet = df_sheet_copy[['날짜', '지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND]].copy()
        for col in [SM_QTY_COL_TREND, SM_WGT_COL_TREND]:
            df_processed_sheet[col] = pd.to_numeric(df_processed_sheet[col], errors='coerce').fillna(0)
        
        df_processed_sheet['지점명'] = df_processed_sheet['지점명'].astype(str).str.strip()
        df_processed_sheet['날짜'] = pd.to_datetime(df_processed_sheet['날짜']).dt.normalize()
        return df_processed_sheet

    except ValueError as ve: # 특정 시트가 없을 때 pd.read_excel에서 발생할 수 있는 오류
        st.warning(f"경고: '{file_name_for_error_msg}' 파일에 '{date_str_yyyymmdd}' 시트를 찾을 수 없거나 읽는 중 오류: {ve}")
        return None
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' 파일의 시트 '{date_str_yyyymmdd}' 처리 중 예외 발생: {e}")
        return None

# 다른 공통 함수들도 필요에 따라 여기에 추가할 수 있습니다.
# 예를 들어, 입고/출고 로그 파일을 처리하는 범용 함수 등
