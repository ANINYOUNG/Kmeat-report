# common_utils.py (UnhashableParamError 수정)

import streamlit as st
import pandas as pd
import datetime
import io 
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from googleapiclient.discovery import Resource # Resource 타입을 명시적으로 임포트

# --- 1. 공통 Google Drive 파일 ID 정의 (예시) ---
# 실제 파일 ID는 메인 앱이나 각 페이지에서 불러와서 함수에 전달하는 것이 더 유연할 수 있습니다.
# 또는, 앱 전체에서 고정적으로 사용되는 파일 ID라면 여기에 정의할 수 있습니다.
# 예시: SM_FILE_ID = "YOUR_ACTUAL_SM_FILE_ID_HERE"
# 예시: ERP_FILE_ID = "YOUR_ACTUAL_ERP_FILE_ID_HERE"

# --- 2. "창고별 재고 추이" 페이지 (inventory_app.py) 특화 설정 ---
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

# googleapiclient.discovery.Resource 타입의 객체는 기본적으로 해시 불가능하므로,
# Streamlit의 캐싱 메커니즘이 이를 무시하도록 hash_funcs를 설정합니다.
# 이렇게 하면 drive_service 객체의 내용 대신 ID (또는 None)를 기반으로 캐시하게 되어
# UnhashableParamError를 방지할 수 있습니다.
# 이 방식은 drive_service 객체 자체가 변경되지 않는 한 안전합니다.
def hash_google_api_resource(resource_obj: Resource):
    # drive_service 객체의 해시는 항상 None을 반환하여
    # 캐시 키 생성 시 이 객체의 내용을 직접 해시하지 않도록 합니다.
    # 즉, 이 객체는 캐시 키에 영향을 주지 않습니다.
    # 함수 호출 시 다른 인자들(file_id 등)이 동일하면 캐시된 결과를 사용합니다.
    return None

@st.cache_data(ttl=300, hash_funcs={Resource: hash_google_api_resource})
def download_excel_from_drive_as_bytes(drive_service: Resource, file_id: str, file_name_for_error_msg: str = "Excel file") -> io.BytesIO | None:
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
            # st.write(f"다운로드 진행률: {int(status.progress() * 100)}%") # 필요시 진행률 표시
        fh.seek(0)
        return fh
    except HttpError as error:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 다운로드 실패: {error.resp.status} - {error._get_reason()}. 파일 공유 설정을 확인하세요.")
        return None
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' (ID: {file_id}) 파일 처리 중 예외 발생: {e}")
        return None

@st.cache_data(ttl=300) # 파일 내용 기반 캐싱이므로 drive_service는 직접 받지 않음
def get_all_available_sheet_dates_from_bytes(file_content_bytes: io.BytesIO | None, file_name_for_error_msg: str = "Excel file") -> list:
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
        # BytesIO 객체는 파일처럼 바로 읽을 수 있습니다.
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

@st.cache_data(ttl=300, hash_funcs={Resource: hash_google_api_resource})
def load_sm_sheet_data(drive_service: Resource, file_id: str, date_str_yyyymmdd: str, file_name_for_error_msg: str = "SM재고현황") -> pd.DataFrame | None:
    """
    지정된 Google Drive 파일 ID에서 특정 날짜(YYYYMMDD)의 시트 데이터를 DataFrame으로 로드합니다.
    """
    file_bytes = download_excel_from_drive_as_bytes(drive_service, file_id, f"{file_name_for_error_msg} ({date_str_yyyymmdd})")
    if file_bytes is None:
        return None # 파일 다운로드 실패

    try:
        df_sheet = pd.read_excel(file_bytes, sheet_name=date_str_yyyymmdd, header=0)
        df_sheet.dropna(how='all', inplace=True)
        if df_sheet.empty:
            return pd.DataFrame() # 빈 데이터프레임 반환

        required_cols = ['지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND]
        if not all(col in df_sheet.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_sheet.columns]
            st.warning(f"경고: '{file_name_for_error_msg}' 파일의 '{date_str_yyyymmdd}' 시트에 필수 컬럼 {missing} 중 일부가 누락되었습니다.")
            return pd.DataFrame() 

        df_sheet_copy = df_sheet.copy()
        df_sheet_copy['날짜'] = pd.to_datetime(date_str_yyyymmdd, format='%Y%m%d')
        
        df_processed_sheet = df_sheet_copy[['날짜', '지점명', '상품코드', SM_QTY_COL_TREND, SM_WGT_COL_TREND]].copy()
        for col in [SM_QTY_COL_TREND, SM_WGT_COL_TREND]:
            df_processed_sheet[col] = pd.to_numeric(df_processed_sheet[col], errors='coerce').fillna(0)
        
        df_processed_sheet['지점명'] = df_processed_sheet['지점명'].astype(str).str.strip()
        df_processed_sheet['날짜'] = pd.to_datetime(df_processed_sheet['날짜']).dt.normalize()
        return df_processed_sheet

    except ValueError as ve: 
        st.warning(f"경고: '{file_name_for_error_msg}' 파일에 '{date_str_yyyymmdd}' 시트를 찾을 수 없거나 읽는 중 오류: {ve}")
        return None
    except Exception as e:
        st.error(f"오류: '{file_name_for_error_msg}' 파일의 시트 '{date_str_yyyymmdd}' 처리 중 예외 발생: {e}")
        return None

# 다른 공통 함수들도 필요에 따라 여기에 추가할 수 있습니다.
# 예를 들어, 입고/출고 로그 파일을 처리하는 범용 함수 등
# 만약 다른 함수들도 drive_service를 인자로 받고 @st.cache_data를 사용한다면,
# 동일하게 hash_funcs={Resource: hash_google_api_resource}를 추가해야 합니다.
