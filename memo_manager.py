# memo_manager.py (Sticky Note Component 관리 모듈)

import streamlit as st
import json
import io
import uuid
import datetime
import os
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# --- 메모 데이터 로딩 ---
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
        # 파일이 없는 경우 등, 에러 발생 시 빈 리스트 반환
        return []
    except Exception:
        return []

# --- 메모 데이터 저장 ---
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

# --- Sticky Note 컴포넌트 렌더링 ---
def render_sticky_notes(memo_file_id):
    """HTML 컴포넌트를 사용하여 Sticky Notes를 렌더링합니다."""

    st.markdown("---")
    st.subheader("📝 포스트잇 메모")

    current_drive_service = st.session_state.get('drive_service')
    if not current_drive_service:
        st.warning("Drive 서비스가 연결되지 않아 메모 기능을 사용할 수 없습니다.")
        return

    # 1. 세션 상태에 메모 데이터 로드 (최초 1회)
    if 'memos' not in st.session_state:
        st.session_state.memos = load_memos_from_drive(current_drive_service, memo_file_id)
    
    # 2. 새 메모 추가 버튼
    if st.button("새 포스트잇 추가", use_container_width=True):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_memo = {
            "id": str(uuid.uuid4()),
            "content": "여기에 내용을 입력하세요...",
            "timestamp": now,
            "x": 50, # 새 메모 초기 위치
            "y": 50,
        }
        st.session_state.memos.append(new_memo)
        save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
    
    # 3. HTML 컴포넌트 파일 읽기
    component_path = "sticky_notes_component.html"
    if not os.path.exists(component_path):
        st.error(f"{component_path} 파일을 찾을 수 없습니다.")
        return
        
    with open(component_path, 'r', encoding='utf-8') as f:
        html_template = f.read()

    # 4. 컴포넌트 렌더링 및 데이터 통신
    component_data = {"memos": st.session_state.memos}
    updated_memos = st.components.v1.html(
        html_template, 
        width=None, 
        height=600, # 메모 보드 높이
        scrolling=True
    )

    # 5. JS로부터 데이터가 오면 (메모가 변경되면) 처리
    if updated_memos:
        if st.session_state.memos != updated_memos:
            st.session_state.memos = updated_memos
            save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
