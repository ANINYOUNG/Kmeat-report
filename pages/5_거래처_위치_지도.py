# pages/5_거래처_위치_지도.py

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium # st_folium을 직접 사용하지 않는다면 제거 가능
from io import BytesIO
from datetime import datetime
# import os # os.path 관련 함수는 직접 사용하지 않도록 수정

# common_utils.py 에서 공통 유틸리티 함수 가져오기
# DATA_FOLDER는 더 이상 common_utils에서 가져오지 않음 (로컬 경로 의존성 제거)
try:
    from common_utils import download_excel_from_drive_as_bytes
    COMMON_UTILS_LOADED = True
except ImportError:
    st.error("오류: common_utils.py 파일을 찾을 수 없거나, 해당 파일에서 필요한 함수를 가져올 수 없습니다.")
    COMMON_UTILS_LOADED = False
    # 이 경우 앱 실행이 어려우므로 중단하거나, 대체 로직 필요
    # st.stop() 

# --- Google Drive 파일 ID 정의 ---
# 사용자님이 제공해주신 실제 파일 ID를 사용합니다.
CUSTOMER_DATA_FILE_ID = "1t1ORfuuHfW3VZ0yXTiIaaBgHzYF8MDwd" # 거래처주소업데이트_완료.xlsx 파일 ID
# --- 파일 ID 정의 끝 ---

# --- 이 페이지에서 사용할 상수 정의 ---
# CUSTOMER_DATA_FILENAME = '거래처주소업데이트_완료.xlsx' # 파일 이름은 오류 메시지 등에 사용 가능
# LAST_UPDATE_FILENAME = 'map_data_last_update.txt' # 클라우드에서 파일 기반 업데이트 시간 저장은 어려움

REQUIRED_EXCEL_COLS = ['거래처명', '주소', '위도', '경도', '담당자']
MANAGER_COL = '담당자' 
REFRIGERATED_WAREHOUSE_KEYWORD = "냉창" 

# --- Google Drive 서비스 객체 가져오기 ---
retrieved_drive_service = st.session_state.get('drive_service')
page_title_for_debug = "거래처 위치 지도 페이지" 

if retrieved_drive_service:
    st.sidebar.info(f"'{page_title_for_debug}'에서 Drive Service 로드 성공!")
else:
    st.sidebar.error(f"'{page_title_for_debug}'에서 Drive Service 로드 실패! (None). 메인 페이지를 먼저 방문하여 인증을 완료해주세요.")

drive_service = retrieved_drive_service

# --- 데이터 관련 함수 (Google Drive 연동으로 수정) ---

# 최종 업데이트 시간은 st.session_state를 사용하거나, 데이터 로드 시점으로 대체
def get_last_update_display():
    if 'map_data_last_df_load_time' in st.session_state:
        return f"현재 세션 데이터 로드: {st.session_state['map_data_last_df_load_time']}"
    if 'map_data_last_upload_processed_time' in st.session_state: # 업로드 처리 시간
        return f"업로드 처리: {st.session_state['map_data_last_upload_processed_time']} (현재 세션만 적용)"
    return "정보 없음 (또는 메인에서 로드 필요)"

@st.cache_data(ttl=300, hash_funcs={"googleapiclient.discovery.Resource": lambda _: None})
def load_customer_data(_drive_service, file_id_customer):
    """거래처 데이터를 Google Drive에서 로드하고 기본 전처리를 수행합니다."""
    if not COMMON_UTILS_LOADED: # common_utils 로드 실패 시
        st.error("필수 유틸리티(common_utils.py) 로드 실패로 데이터를 가져올 수 없습니다.")
        return None
        
    if _drive_service is None:
        st.error("오류: Google Drive 서비스가 초기화되지 않았습니다. (거래처 데이터 로딩)")
        return None

    file_bytes_customer = download_excel_from_drive_as_bytes(_drive_service, file_id_customer, "거래처주소데이터")
    if file_bytes_customer is None:
        return None # 오류 메시지는 download 함수에서 표시
        
    try:
        df = pd.read_excel(file_bytes_customer)
        
        # 담당자 컬럼이 없어도 다른 필수 컬럼은 확인해야 함
        temp_required_cols = [col for col in REQUIRED_EXCEL_COLS if col != MANAGER_COL]
        missing_cols = [col for col in temp_required_cols if col not in df.columns]
        if missing_cols:
            st.error(f"거래처 데이터 파일 (ID: {file_id_customer})에 필수 컬럼이 없습니다: {missing_cols}. ({', '.join(temp_required_cols)} 필요)")
            return None
        
        # 담당자 컬럼이 없으면 빈 컬럼으로 추가 (오류 방지 및 하위 로직 호환성)
        if MANAGER_COL not in df.columns:
            st.info(f"거래처 데이터 파일에 '{MANAGER_COL}' 컬럼이 없어 빈 값으로 추가합니다. '냉창' 여부 표시에 영향이 있을 수 있습니다.")
            df[MANAGER_COL] = ""
            
        df['위도'] = pd.to_numeric(df['위도'], errors='coerce')
        df['경도'] = pd.to_numeric(df['경도'], errors='coerce')
        df.dropna(subset=['위도', '경도'], inplace=True) # 위도, 경도 없는 데이터는 지도에 표시 불가
        
        if df.empty:
            st.warning(f"거래처 데이터 파일 (ID: {file_id_customer})에 유효한 위도/경도 데이터가 없습니다.")
            return pd.DataFrame() # 빈 DataFrame 반환

        df['거래처명'] = df['거래처명'].astype(str).str.strip()
        df['주소'] = df['주소'].astype(str).str.strip().fillna("주소 정보 없음")
        df[MANAGER_COL] = df[MANAGER_COL].astype(str).str.strip().fillna("") 
        
        # 데이터 로드 성공 시, 현재 시간을 세션 상태에 기록 (업데이트 시간 표시용)
        st.session_state['map_data_last_df_load_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'map_data_last_upload_processed_time' in st.session_state: # 이전 업로드 기록이 있다면 삭제
            del st.session_state['map_data_last_upload_processed_time']

        return df
    except Exception as e:
        st.error(f"거래처 데이터 (ID: {file_id_customer}) 로드 중 오류 발생: {e}")
        return None

def process_uploaded_customer_data(new_file_bytes):
    """업로드된 엑셀 파일 바이트를 DataFrame으로 변환하고 기본 처리합니다. (Google Drive에 저장하지 않음)"""
    try:
        with BytesIO(new_file_bytes) as f:
            df_new = pd.read_excel(f)
        
        temp_required_cols = [col for col in REQUIRED_EXCEL_COLS if col != MANAGER_COL]
        missing_cols = [col for col in temp_required_cols if col not in df_new.columns]
        if missing_cols:
            st.error(f"업로드한 파일에 필수 컬럼이 없습니다: {missing_cols}. ({', '.join(temp_required_cols)} 필요)")
            return None
        
        if MANAGER_COL not in df_new.columns:
            st.info(f"업로드한 파일에 '{MANAGER_COL}' 컬럼이 없어 빈 값으로 추가합니다.")
            df_new[MANAGER_COL] = ""

        df_new['위도'] = pd.to_numeric(df_new['위도'], errors='coerce')
        df_new['경도'] = pd.to_numeric(df_new['경도'], errors='coerce')
        df_new.dropna(subset=['위도', '경도'], inplace=True)

        if df_new.empty:
            st.warning("업로드한 파일에 유효한 위도/경도 데이터가 없습니다.")
            return pd.DataFrame()

        df_new['거래처명'] = df_new['거래처명'].astype(str).str.strip()
        df_new['주소'] = df_new['주소'].astype(str).str.strip().fillna("주소 정보 없음")
        df_new[MANAGER_COL] = df_new[MANAGER_COL].astype(str).str.strip().fillna("")

        # 업로드된 데이터 처리 성공 시, 현재 시간을 세션 상태에 기록
        st.session_state['map_data_last_upload_processed_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'map_data_last_df_load_time' in st.session_state: # Drive 로드 기록이 있다면 삭제
            del st.session_state['map_data_last_df_load_time']
        
        st.cache_data.clear() # 새 데이터 반영 위해 캐시 클리어
        return df_new
    except Exception as e:
        st.error(f"업로드된 데이터 처리 중 오류 발생: {e}")
        return None

# --- Streamlit 페이지 UI 구성 ---
st.title("🗺️ 거래처 위치 지도")

if drive_service is None: 
    st.error("Google Drive 서비스에 연결되지 않았습니다. 앱의 메인 페이지를 방문하여 인증을 완료하거나, 앱 설정을 확인해주세요.")
    st.stop()
if not COMMON_UTILS_LOADED: # common_utils 로드 실패 시
    st.error("페이지 실행에 필요한 유틸리티(common_utils.py)를 로드하지 못했습니다.")
    st.stop()

st.markdown("Google Drive의 엑셀 파일 데이터를 기반으로 거래처 위치를 지도에 표시합니다.")
st.markdown(f"데이터 파일 ID: `{CUSTOMER_DATA_FILE_ID}`")
st.markdown("---")

# 세션 상태에 업로드된 데이터가 있으면 그것을 사용, 없으면 Drive에서 로드
if 'uploaded_customer_df' in st.session_state and st.session_state.uploaded_customer_df is not None:
    df_customers = st.session_state.uploaded_customer_df
    st.info("업로드된 파일의 데이터로 지도를 표시합니다 (현재 세션에만 적용).")
else:
    df_customers = load_customer_data(drive_service, CUSTOMER_DATA_FILE_ID)

last_update_display = get_last_update_display()

# --- 사이드바 ---
st.sidebar.header('데이터 관리')
st.sidebar.write(f"데이터 상태: {last_update_display}")
uploaded_file = st.sidebar.file_uploader(f"'거래처주소업데이트_완료.xlsx' 형식 파일 업로드 (현재 세션에만 적용)", type=['xlsx'], key="customer_map_uploader")

if uploaded_file is not None:
    uploaded_file_bytes = uploaded_file.getvalue()
    df_processed_upload = process_uploaded_customer_data(uploaded_file_bytes)
    if df_processed_upload is not None:
        st.session_state.uploaded_customer_df = df_processed_upload # 세션 상태에 저장
        df_customers = df_processed_upload # 현재 표시할 데이터프레임도 업데이트
        st.sidebar.success(f'업로드된 파일이 처리되었습니다.\n(처리 시간: {get_last_update_display()})')
        # st.experimental_rerun() # 페이지를 다시 실행하여 변경사항 즉시 반영
        st.rerun() # 최신 Streamlit에서는 st.rerun() 사용
    else:
        st.sidebar.error("업로드된 파일 처리 중 오류가 발생했습니다.")

# ... (이하 기존 사이드바 검색 로직 및 지도 표시 로직은 df_customers를 사용하므로 큰 변경 없이 유지 가능) ...
# ... (다만, df_customers가 None이거나 비어있을 경우에 대한 처리는 강화하는 것이 좋음) ...

st.sidebar.markdown("---")
st.sidebar.header("거래처 정보 검색 (참고용)")
search_customer_name = st.sidebar.text_input("거래처명으로 검색", key="search_cust_by_name_sidebar")
if search_customer_name and df_customers is not None and not df_customers.empty:
    searched_by_name_df = df_customers[df_customers['거래처명'].str.contains(search_customer_name.strip(), case=False, na=False, regex=False)]
    if not searched_by_name_df.empty:
        st.sidebar.markdown("**거래처명 검색 결과:**")
        for idx, row in searched_by_name_df.head().iterrows(): 
            st.sidebar.markdown(f"**{row['거래처명']}**")
            st.sidebar.markdown(f" 주소: {row['주소']}")
            if MANAGER_COL in row and pd.notna(row[MANAGER_COL]) and row[MANAGER_COL] != "":
                st.sidebar.markdown(f" 담당자: {row[MANAGER_COL]}")
            st.sidebar.markdown("---")
        if len(searched_by_name_df) > 5:
            st.sidebar.caption(f"... 외 {len(searched_by_name_df) - 5}건 더 있음")
    elif search_customer_name: 
        st.sidebar.info(f"거래처명 '{search_customer_name}'에 대한 검색 결과가 없습니다.")

st.sidebar.markdown("---") 
st.sidebar.header("주소로 거래처 찾기 (지도에 즉시 표시)")
search_address = st.sidebar.text_input("주소의 일부 또는 전체 입력", key="search_by_address_map_sidebar")

searched_by_address_df_for_map = pd.DataFrame() 
if search_address and df_customers is not None and not df_customers.empty:
    search_address_stripped = search_address.strip()
    if search_address_stripped: 
        searched_by_address_df_for_map = df_customers[df_customers['주소'].str.contains(search_address_stripped, case=False, na=False, regex=False)]
        if not searched_by_address_df_for_map.empty:
            st.sidebar.markdown(f"**'{search_address_stripped}' 포함 주소 검색 결과 ({len(searched_by_address_df_for_map)}건):**")
            for idx, row in searched_by_address_df_for_map.head().iterrows():
                st.sidebar.markdown(f"- **{row['거래처명']}**: {row['주소']}")
            if len(searched_by_address_df_for_map) > 5:
                st.sidebar.caption(f"... 외 {len(searched_by_address_df_for_map) - 5}건 더 있음")
            st.sidebar.markdown("---")
            st.sidebar.info("검색된 거래처들이 지도에 다른 색상으로 표시됩니다.")
        else: 
            st.sidebar.info(f"주소 '{search_address_stripped}'를 포함하는 거래처 정보가 없습니다.")

if df_customers is None or df_customers.empty:
    st.warning("거래처 데이터를 불러올 수 없거나 데이터가 없습니다. 사이드바에서 파일을 업로드하거나 Google Drive 파일 ID 및 공유 설정을 확인해주세요.")
    st.stop()

# '케이미트'를 찾아서 지도 중심으로 설정 (기존 로직 유지)
keimeat_row = df_customers[df_customers['거래처명'] == '케이미트']
if not keimeat_row.empty:
    keimeat_coords = (keimeat_row.iloc[0]['위도'], keimeat_row.iloc[0]['경도'])
    map_center = keimeat_coords
    zoom_level = 12
else:
    st.warning("'케이미트' 정보를 찾을 수 없어 거래처 평균 위치로 지도를 표시합니다. '케이미트'를 데이터에 추가해주세요.")
    # df_customers가 비어있지 않음을 위에서 확인했으므로 mean() 사용 가능
    map_center = (df_customers['위도'].mean(), df_customers['경도'].mean()) 
    zoom_level = 10

# Folium 지도 생성
groups = {'박용신': 'green', '정종환': 'blue', '이주현': 'purple', '조성균': 'orange', '윤성한': 'yellow'}
m = folium.Map(location=map_center, zoom_start=zoom_level, tiles="cartodbpositron")

# 케이미트 마커 추가
if not keimeat_row.empty:
    folium.Marker(
        keimeat_coords,
        icon=folium.Icon(color='red', icon='home', prefix='fa'),
        tooltip='<strong>케이미트 본사</strong>',
        popup=folium.Popup(f"<b>케이미트</b><br>주소: {keimeat_row.iloc[0]['주소']}<br>({keimeat_coords[0]:.4f}, {keimeat_coords[1]:.4f})", max_width=300)
    ).add_to(m)

# 차고지 마커 추가
for group_name, color_code in groups.items():
    garage_row = df_customers[df_customers['거래처명'] == group_name] 
    if not garage_row.empty:
        garage_location = garage_row.iloc[0]
        garage_coords = (garage_location['위도'], garage_location['경도'])
        folium.Marker(
            garage_coords,
            icon=folium.Icon(color='black', icon='flag', prefix='fa'), 
            tooltip=f'<strong>{group_name} 차고지</strong>',
            popup=folium.Popup(f"<b>{group_name} 차고지</b><br>주소: {garage_location['주소']}<br>({garage_coords[0]:.4f}, {garage_coords[1]:.4f})", max_width=300)
        ).add_to(m)

st.sidebar.header("그룹별 배송 루트 설정 (지도 표시)")
selected_customers_to_display = pd.DataFrame()
has_manager_col = MANAGER_COL in df_customers.columns
base_available_customers_df = df_customers[~df_customers['거래처명'].isin(list(groups.keys()) + ['케이미트'])].copy()
all_selectable_customer_names = sorted(list(base_available_customers_df['거래처명'].unique()))

for group_name, color_code in groups.items():
    with st.sidebar.expander(f"{group_name} 그룹 경로 거래처 선택", expanded=False):
        if all_selectable_customer_names:
            selected_names_for_group = st.multiselect(
                f'{group_name} 그룹의 배송 거래처를 선택하세요:',
                options=all_selectable_customer_names,
                key=f"multiselect_route_{group_name}"
            )
            if selected_names_for_group:
                group_route_customers_df = df_customers[df_customers['거래처명'].isin(selected_names_for_group)].copy()
                group_route_customers_df['그룹'] = group_name
                group_route_customers_df['색상'] = color_code
                selected_customers_to_display = pd.concat([selected_customers_to_display, group_route_customers_df])
        else:
            st.caption("선택할 수 있는 일반 거래처 데이터가 없습니다.")

# 지도에 마커 추가 로직 (주소 검색 결과 및 그룹 경로)
# 1. 주소 검색 결과 마커
if not searched_by_address_df_for_map.empty:
    for idx, row in searched_by_address_df_for_map.iterrows():
        search_marker_color = 'cadetblue' 
        search_marker_icon = 'info-circle'
        if has_manager_col and row.get(MANAGER_COL, "") == REFRIGERATED_WAREHOUSE_KEYWORD:
            search_marker_color = 'darkblue'
            search_marker_icon = 'warehouse'
        folium.Marker(
            location=(row['위도'], row['경도']),
            tooltip=f"<strong>{row['거래처명']} (주소 검색됨)</strong><br>주소: {row['주소']}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}",
            popup=folium.Popup(f"<b>{row['거래처명']}</b><br>주소: {row['주소']}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}<br>({row['위도']:.4f}, {row['경도']:.4f})", max_width=300),
            icon=folium.Icon(color=search_marker_color, icon=search_marker_icon, prefix='fa')
        ).add_to(m)

# 2. 그룹 경로 설정 결과 마커
if not selected_customers_to_display.empty:
    for idx, row in selected_customers_to_display.iterrows():
        is_already_in_address_search = False
        if not searched_by_address_df_for_map.empty:
            if row['거래처명'] in searched_by_address_df_for_map['거래처명'].values:
                is_already_in_address_search = True
        
        if not is_already_in_address_search:
            business_coords = (row['위도'], row['경도'])
            customer_name = row['거래처명']
            customer_address = row['주소']
            group_color_for_marker = row['색상']
            marker_color_group_route = group_color_for_marker 
            marker_icon_group_route = 'truck'
            
            if has_manager_col and row.get(MANAGER_COL, "") == REFRIGERATED_WAREHOUSE_KEYWORD:
                marker_color_group_route = 'darkblue'
                marker_icon_group_route = 'warehouse'
            
            folium.Marker(
                business_coords,
                icon=folium.Icon(color=marker_color_group_route, icon=marker_icon_group_route, prefix='fa'),
                tooltip=f"<strong>{customer_name}</strong><br>그룹: {row['그룹']}<br>주소: {customer_address}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}",
                popup=folium.Popup(f"<b>{customer_name}</b><br>주소: {customer_address}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}<br>({business_coords[0]:.4f}, {business_coords[1]:.4f})", max_width=300)
            ).add_to(m)
            
            text_border_color = 'darkblue' if marker_color_group_route == 'darkblue' else marker_color_group_route
            folium.map.Marker(
                business_coords,
                icon=folium.DivIcon(html=f"""
                    <div style="
                        position: absolute; transform: translate(-50%, -120%); 
                        font-size: 10px; color: black; font-weight: bold;
                        background-color: rgba(255, 255, 255, 0.85); 
                        border: 1px solid {text_border_color}; border-radius: 3px;
                        padding: 1px 3px; white-space: nowrap; 
                    ">{customer_name}</div>""")
            ).add_to(m)

if searched_by_address_df_for_map.empty and selected_customers_to_display.empty:
    if not search_address and not any(st.session_state.get(f"multiselect_route_{g}") for g in groups.keys()):
        st.info("사이드바에서 주소로 특정 거래처를 검색하거나, 그룹별로 배송 루트를 설정하면 지도에 표시됩니다. '케이미트' 본사와 각 그룹의 차고지는 기본으로 표시됩니다.")

# st_folium 함수를 사용하여 지도 표시
# returned_objects=[]는 지도에서 클릭 등의 상호작용 결과를 받지 않겠다는 의미
st_folium(m, width='100%', height=600, returned_objects=[])

