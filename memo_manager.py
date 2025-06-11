# memo_manager.py (메모 기능 모듈)

import streamlit as st
import json
import io
import uuid
import datetime
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
    except HttpError as error:
        st.error(f"메모 로딩 실패 (HTTP Error): {error}")
        return []
    except Exception as e:
        st.error(f"메모 로딩 중 예외 발생: {e}")
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
        st.toast("메모가 저장되었습니다.", icon="✅")
    except Exception as e:
        st.error(f"메모 저장 실패: {e}")

# --- 메모 보드 UI 렌더링 ---
def render_memo_board(memo_file_id):
    """메인 콘텐츠 영역에 메모 보드를 렌더링합니다."""
    
    st.markdown("---")
    with st.expander("📝 메모 보드 보기/숨기기", expanded=True):
        
        current_drive_service = st.session_state.get('drive_service')
        if not current_drive_service:
            st.warning("Drive 서비스가 연결되지 않아 메모 기능을 사용할 수 없습니다.")
            return

        # 세션 상태 초기화 (최초 1회만 실행)
        if 'memos' not in st.session_state:
            st.session_state.memos = load_memos_from_drive(current_drive_service, memo_file_id)
        if 'editing_memo_id' not in st.session_state:
            st.session_state.editing_memo_id = None

        # --- 새 메모 작성 UI ---
        st.subheader("새 메모 추가")
        new_memo_content = st.text_area("내용:", key="new_memo_board_text", label_visibility="collapsed")
        if st.button("메모 추가", use_container_width=True):
            if new_memo_content:
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                new_memo = {
                    "id": str(uuid.uuid4()),
                    "content": new_memo_content,
                    "timestamp": now
                }
                # 새 메모를 항상 맨 앞에 추가
                st.session_state.memos.insert(0, new_memo)
                save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
                # 입력창 비우기 (스크립트 재실행을 유도하지 않음)
                st.session_state.new_memo_board_text = ""

        st.markdown("---")

        # --- 메모 목록 (포스트잇 보드 형식) ---
        if not st.session_state.memos:
            st.info("작성된 메모가 없습니다.")
        else:
            # 한 줄에 3개씩 메모 표시
            cols = st.columns(3)
            for i, memo in enumerate(st.session_state.memos):
                with cols[i % 3]:
                    with st.container(border=True):
                        memo_id = memo['id']

                        # 수정 모드
                        if st.session_state.editing_memo_id == memo_id:
                            edited_content = st.text_area(
                                "수정:",
                                value=memo['content'],
                                key=f"edit_text_{memo_id}",
                                height=150
                            )
                            b_cols = st.columns(2)
                            if b_cols[0].button("저장", key=f"save_{memo_id}", use_container_width=True):
                                memo['content'] = edited_content
                                st.session_state.editing_memo_id = None
                                save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)

                            if b_cols[1].button("취소", key=f"cancel_{memo_id}", use_container_width=True):
                                st.session_state.editing_memo_id = None
                        
                        # 일반 표시 모드
                        else:
                            st.markdown(f"<div style='min-height: 120px;'>{memo['content']}</div>", unsafe_allow_html=True)
                            st.caption(f"작성: {memo['timestamp']}")
                            
                            b_cols = st.columns(2)
                            if b_cols[0].button("수정", key=f"edit_{memo_id}", use_container_width=True):
                                st.session_state.editing_memo_id = memo_id
                            
                            if b_cols[1].button("삭제", key=f"delete_{memo_id}", use_container_width=True):
                                st.session_state.memos = [m for m in st.session_state.memos if m['id'] != memo_id]
                                save_memos_to_drive(current_drive_service, memo_file_id, st.session_state.memos)
