# inventory_app.py (st.session_state 디버깅용 간략 버전)

import streamlit as st

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="데이터 분석 대시보드", layout="wide", initial_sidebar_state="expanded")

# --- 2. 기본 라이브러리 임포트 ---
import json # JSON 처리
import os   # 운영체제 관련 기능 (파일 경로 등)
# 다음 라이브러리들은 메인 페이지의 간략한 테스트에서는 직접 사용되지 않을 수 있으나,
# common_utils나 다른 부분에서 필요할 수 있으므로 일단 포함합니다.
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta # 날짜 계산
import io # 바이트 스트림 처리
import plotly.express as px # Plotly 차트 (간략 버전에서는 미사용)

# --- 3. Google Drive API 관련 라이브러리 임포트 ---
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaIoBaseDownload # common_utils.py로 이동했을 수 있음

# --- 4. common_utils.py 에서 함수 및 상수 가져오기 ---
# (common_utils.py가 프로젝트 루트에 있고, Streamlit Cloud 환경에 맞게 수정되었다고 가정)
COMMON_UTILS_LOADED = False
try:
    from common_utils import (
        download_excel_from_drive_as_bytes,
        get_all_available_sheet_dates_from_bytes
        # 메인 페이지 테스트에 필요한 다른 함수나 상수가 있다면 여기에 추가
    )
    COMMON_UTILS_LOADED = True
    # st.sidebar.info("common_utils.py 로드 성공.") # 디버깅용 메시지 (필요시 주석 해제)
except ImportError:
    st.error("오류: common_utils.py 파일을 찾을 수 없거나, 해당 파일에서 필요한 함수를 가져올 수 없습니다. 파일 위치와 내용을 확인해주세요.")
    # COMMON_UTILS_LOADED는 False로 유지됨

# --- 5. Google API 인증 및 Drive 서비스 클라이언트 생성 ---
drive_service = None  # drive_service 변수 초기화
SERVICE_ACCOUNT_LOADED = False # 인증 성공 여부 플래그

# Streamlit Cloud 환경인지 판단 (st.secrets에 키가 있는지 확인)
IS_CLOUD_ENVIRONMENT = "google_creds_json" in st.secrets

if IS_CLOUD_ENVIRONMENT:
    # st.sidebar.info("클라우드 환경으로 판단됨. st.secrets에서 인증 정보 로드 시도.") # 상세 디버깅 메시지
    try:
        creds_json_str = st.secrets["google_creds_json"]
        creds_dict = json.loads(creds_json_str) # JSON 문자열을 딕셔너리로 변환
        scopes = ['https://www.googleapis.com/auth/drive.readonly'] # API 접근 범위
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        drive_service = build('drive', 'v3', credentials=creds) # Google Drive 서비스 객체 생성
        SERVICE_ACCOUNT_LOADED = True # 인증 성공 플래그 설정
        # st.sidebar.success("클라우드: Google Drive 서비스 초기화 성공!") # 성공 메시지
    except Exception as e_secrets:
        st.sidebar.error(f"클라우드 Secrets 인증 중 심각한 오류 발생: {e_secrets}")
        drive_service = None # 오류 발생 시 None으로 명시적 설정
        SERVICE_ACCOUNT_LOADED = False
else:
    # 로컬 개발 환경일 경우
    # st.sidebar.info("로컬 환경으로 판단됨. 로컬 서비스 계정 키 파일에서 인증 정보 로드 시도.")
    # !!! 로컬 테스트 시 실제 서비스 계정 키 파일 경로로 반드시 수정해야 합니다 !!!
    SERVICE_ACCOUNT_FILE_PATH = "YOUR_LOCAL_SERVICE_ACCOUNT_FILE_PATH.json" 
    # 예시: SERVICE_ACCOUNT_FILE_PATH = r"C:\path\to\your\service_account_key.json"
    
    if os.path.exists(SERVICE_ACCOUNT_FILE_PATH):
        try:
            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE_PATH, scopes=scopes)
            drive_service = build('drive', 'v3', credentials=creds)
            SERVICE_ACCOUNT_LOADED = True
            # st.sidebar.success(f"로컬: 서비스 계정 키 파일({os.path.basename(SERVICE_ACCOUNT_FILE_PATH)}) 로드 성공!")
        except Exception as e_local:
            st.sidebar.error(f"로컬 서비스 계정 키 파일 인증 중 오류: {e_local}")
            drive_service = None
            SERVICE_ACCOUNT_LOADED = False
    else:
        st.sidebar.warning(f"로컬: 서비스 계정 키 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE_PATH}. Google Drive 연동이 불가능합니다.")
        drive_service = None
        SERVICE_ACCOUNT_LOADED = False

# --- 6. st.session_state에 drive_service 저장 및 디버깅 메시지 ---
# 이 부분은 앱이 실행될 때마다 (페이지 이동 포함) 실행될 수 있으므로,
# drive_service가 성공적으로 초기화되었을 때만 st.session_state에 저장합니다.
if SERVICE_ACCOUNT_LOADED and drive_service is not None:
    if 'drive_service' not in st.session_state or st.session_state.get('drive_service') is None:
        st.session_state['drive_service'] = drive_service
        st.sidebar.success("Drive service가 메인 앱 세션에 성공적으로 저장됨!")
    # 이미 저장되어 있고 유효하다면 메시지를 반복해서 띄울 필요는 없음
    # else:
    #     st.sidebar.info("Drive service는 이미 세션에 저장되어 있습니다 (메인 앱).")
elif not SERVICE_ACCOUNT_LOADED or drive_service is None: # 명시적으로 실패한 경우
    st.sidebar.error("메인 앱: Drive service 초기화 실패 또는 인증 정보 없음!")
    if 'drive_service' in st.session_state:
        del st.session_state['drive_service'] # 이전 실행의 잔여 값 또는 실패한 값 제거

# --- 7. 메인 페이지 UI (최소화된 버전) ---
st.title("📊 데이터 분석 대시보드 (메인 - 간략 버전)")

# st.session_state에서 drive_service를 가져와서 상태 확인
current_drive_service_in_session = st.session_state.get('drive_service')

if current_drive_service_in_session is not None:
    st.success("Google Drive 서비스가 연결되었습니다. 사이드바에서 다른 메뉴를 테스트해보세요.")
    
    # 선택 사항: 간단한 테스트용 데이터 로드 시도 (common_utils가 로드되었고, 테스트용 파일 ID가 있을 경우)
    # if COMMON_UTILS_LOADED:
    #     # !!! 테스트용 SM 파일 ID를 실제 ID로 교체해야 합니다 !!!
    #     SM_FILE_ID_TEST = "YOUR_SM_FILE_ID_HERE_FOR_TESTING" 
    #     if SM_FILE_ID_TEST != "YOUR_SM_FILE_ID_HERE_FOR_TESTING": # 실제 ID가 입력되었는지 간단히 확인
    #         st.write(f"테스트용 SM 파일 ID: {SM_FILE_ID_TEST}")
    #         with st.spinner("테스트 SM 파일 로드 중..."):
    #             sm_file_bytes_test = download_excel_from_drive_as_bytes(current_drive_service_in_session, SM_FILE_ID_TEST, "테스트 SM 파일")
    #             if sm_file_bytes_test:
    #                 st.write("테스트 SM 파일 다운로드 성공.")
    #                 available_dates_test = get_all_available_sheet_dates_from_bytes(sm_file_bytes_test, "테스트 SM 파일")
    #                 st.write("테스트 SM 파일에서 사용 가능한 날짜 시트:", available_dates_test)
    #             else:
    #                 st.warning("테스트 SM 파일 다운로드 또는 날짜 시트 분석 실패.")
    #     else:
    #         st.caption("메인 페이지 테스트용 SM 파일 ID가 설정되지 않았습니다.")
else:
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱 설정을 확인하거나 앱을 재시작해주세요.")
    if not IS_CLOUD_ENVIRONMENT:
        st.info(f"로컬 실행 중이라면, 코드 내의 SERVICE_ACCOUNT_FILE_PATH ('{SERVICE_ACCOUNT_FILE_PATH}')가 올바른지, 그리고 해당 파일이 존재하는지 확인해주세요.")

st.markdown("---")
st.write("이 페이지는 `st.session_state`에 `drive_service`를 저장하는지 테스트하기 위한 간략한 버전입니다.")
st.write("사이드바에서 다른 페이지로 이동하여 해당 페이지의 사이드바에 'Drive Service 로드 성공!' 메시지가 나타나는지 확인해주세요.")

# 메인 앱이 실행될 때 특별히 호출할 함수가 있다면 여기에 배치
# if __name__ == "__main__":
#     # 이 블록은 Streamlit 앱에서는 필수는 아닙니다.
#     # st.write("메인 앱 실행됨 (__name__ == '__main__')")
#     pass
