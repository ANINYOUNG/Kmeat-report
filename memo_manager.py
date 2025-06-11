# memo_manager.py (Sticky Note Component 관리 모듈)

import streamlit as st
import json
import io
import uuid
import datetime
import os
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- 데이터 로딩/저장 함수 ---
def load_memos_from_drive(current_drive_service, file_id):
    """Google Drive에서 메모 파일을 읽어옵니다."""
    try:
        request = current_drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        content = fh.getvalue().decode('utf-8')
        return json.loads(content) if content else []
    except HttpError:
        return []
    except Exception as e:
        st.sidebar.error(f"메모 로딩 실패: {e}")
        return []

def save_memos_to_drive(current_drive_service, file_id, memos_data):
    """메모 데이터를 Google Drive에 저장합니다."""
    try:
        memos_json_str = json.dumps(memos_data, indent=4, ensure_ascii=False)
        fh = io.BytesIO(memos_json_str.encode('utf-8'))
        media = MediaIoBaseUpload(fh, mimetype='application/json', resumable=True)
        current_drive_service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        st.toast("메모가 동기화되었습니다.", icon="🔄")
    except Exception as e:
        st.error(f"메모 저장 실패: {e}")

# --- 세션 상태 초기화 함수 ---
def ensure_memos_loaded(current_drive_service, file_id):
    """
    세션 상태에 메모 데이터가 없으면 로드하여 초기화합니다.
    모든 페이지 상단에서 한 번만 호출됩니다.
    """
    if 'memos' not in st.session_state:
        st.session_state.memos = load_memos_from_drive(current_drive_service, file_id)

# --- 사이드바 UI 렌더링 ---
def initialize_memo_sidebar(memo_file_id):
    """
    사이드바에 '새 포스트잇 추가' 버튼을 렌더링합니다.
    이 함수는 'memos'가 세션 상태에 이미 존재한다고 가정합니다.
    """
    current_drive_service = st.session_state.get('drive_service')
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📝 포스트잇 메모")

    if st.sidebar.button("새 포스트잇 추가", use_container_width=True, key="add_memo_button"):
        if not current_drive_service:
            st.sidebar.warning("Drive 서비스에 연결되지 않아 메모를 추가할 수 없습니다.")
            return

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_memo = {
            "id": str(uuid.uuid4()),
            "content": "여기에 내용을 입력하세요...",
            "timestamp": now,
            "x": 20,
            "y": 20,
        }
        # ensure_memos_loaded 함수가 먼저 호출되므로, st.session_state.memos는 항상 존재합니다.
        st.session_state.memos.append(new_memo)
        save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
        # st.rerun()을 호출하지 않아도, 버튼 클릭 후 스크립트가 자동으로 재실행되어 반영됩니다.

# --- 포스트잇 보드 렌더링 ---
def render_sticky_notes(memo_file_id):
    """
    HTML 컴포넌트를 사용하여 메인 화면에 Sticky Notes 보드를 렌더링합니다.
    """
    current_drive_service = st.session_state.get('drive_service')
    if not current_drive_service:
        st.warning("Drive 서비스가 연결되지 않아 메모 기능을 사용할 수 없습니다.")
        return

    component_path = "sticky_notes_component.html"
    
    if not os.path.exists(component_path):
        st.error(f"컴포넌트 파일을 찾을 수 없습니다: '{component_path}'.")
        return
        
    with open(component_path, 'r', encoding='utf-8') as f:
        html_template = f.read()

    if 'memos' not in st.session_state:
        st.session_state.memos = []

    component_data = {"memos": st.session_state.memos}
    
    # 컴포넌트의 key를 정적으로 유지하여 상태가 유지되도록 함 (오류 해결)
    updated_memos = st.components.v1.html(
        html_template, 
        width=None, 
        height=600,
        scrolling=True,
        key="sticky_notes_component" 
    )

    # 컴포넌트로부터 받은 데이터가 유효한 리스트 형식일 때만 상태를 업데이트합니다. (핵심 오류 수정)
    if updated_memos and isinstance(updated_memos, list) and st.session_state.memos != updated_memos:
        st.session_state.memos = updated_memos
        save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
        st.rerun() # 외부 컴포넌트에서 값이 변경되었을 때만 재실행
