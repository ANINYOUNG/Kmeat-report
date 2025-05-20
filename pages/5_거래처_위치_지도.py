# pages/5_거래처_위치_지도.py

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from io import BytesIO
from datetime import datetime
import os

# common_utils.py 에서 DATA_FOLDER 경로 가져오기
try:
    from common_utils import DATA_FOLDER
except ImportError:
    DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'list')
    st.warning(f"common_utils.py를 찾을 수 없어 기본 데이터 폴더 경로를 사용합니다: {DATA_FOLDER}")


# --- 이 페이지에서 사용할 상수 정의 ---
CUSTOMER_DATA_FILENAME = '거래처주소업데이트_완료.xlsx'
CUSTOMER_DATA_FILE_PATH = os.path.join(DATA_FOLDER, CUSTOMER_DATA_FILENAME)
LAST_UPDATE_FILENAME = 'map_data_last_update.txt'
LAST_UPDATE_FILE_PATH = os.path.join(DATA_FOLDER, LAST_UPDATE_FILENAME)

REQUIRED_EXCEL_COLS = ['거래처명', '주소', '위도', '경도', '담당자']
MANAGER_COL = '담당자' 
REFRIGERATED_WAREHOUSE_KEYWORD = "냉창" 

# --- 데이터 관련 함수 ---
def load_last_update_date(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.read().strip()
    return "최종 업데이트 기록 없음"

def save_last_update_date(filepath, date_str):
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True) 
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(date_str)
    except Exception as e:
        st.error(f"최종 업데이트 일자 저장 중 오류: {e}")

@st.cache_data
def load_customer_data(filepath):
    if not os.path.exists(filepath):
        st.error(f"거래처 데이터 파일을 찾을 수 없습니다: {filepath}")
        return None
    try:
        df = pd.read_excel(filepath)
        missing_cols = [col for col in REQUIRED_EXCEL_COLS if col not in df.columns]
        if missing_cols:
            if MANAGER_COL in missing_cols:
                st.warning(f"거래처 데이터 파일에 '{MANAGER_COL}' 컬럼이 없습니다. '냉창' 여부 표시는 적용되지 않을 수 있습니다.")
                df[MANAGER_COL] = "" 
                missing_cols.remove(MANAGER_COL) 
            
            if missing_cols: 
                st.error(f"거래처 데이터 파일에 필수 컬럼이 없습니다: {missing_cols}. ({', '.join(REQUIRED_EXCEL_COLS)} 필요)")
                return None
        
        df['위도'] = pd.to_numeric(df['위도'], errors='coerce')
        df['경도'] = pd.to_numeric(df['경도'], errors='coerce')
        df.dropna(subset=['위도', '경도'], inplace=True)
        df['거래처명'] = df['거래처명'].astype(str).str.strip()
        df['주소'] = df['주소'].astype(str).str.strip().fillna("주소 정보 없음")
        if MANAGER_COL not in df.columns:
             df[MANAGER_COL] = ""
        df[MANAGER_COL] = df[MANAGER_COL].astype(str).str.strip().fillna("") 
        return df
    except Exception as e:
        st.error(f"거래처 데이터 로드 중 오류 발생: {e}")
        return None

def update_customer_data(new_file_bytes, target_filepath):
    try:
        with BytesIO(new_file_bytes) as f:
            df_new = pd.read_excel(f)
        
        missing_cols = [col for col in REQUIRED_EXCEL_COLS if col not in df_new.columns]
        if missing_cols:
            if MANAGER_COL in missing_cols:
                st.warning(f"업로드한 파일에 '{MANAGER_COL}' 컬럼이 없습니다. '냉창' 정보 표시에 영향이 있을 수 있습니다.")
                df_new[MANAGER_COL] = ""
                missing_cols.remove(MANAGER_COL)
            if missing_cols:
                st.error(f"업로드한 파일에 필수 컬럼이 없습니다: {missing_cols}. ({', '.join(REQUIRED_EXCEL_COLS)} 필요)")
                return None, load_last_update_date(LAST_UPDATE_FILE_PATH)

        df_new.to_excel(target_filepath, index=False)
        current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_last_update_date(LAST_UPDATE_FILE_PATH, current_time_str)
        st.cache_data.clear() 
        return load_customer_data(target_filepath), current_time_str
    except Exception as e:
        st.error(f"데이터 업데이트 중 오류 발생: {e}")
        return None, load_last_update_date(LAST_UPDATE_FILE_PATH)

# --- Streamlit 페이지 UI 구성 ---
st.title("🗺️ 거래처 위치 지도")
st.markdown("엑셀 파일의 데이터를 기반으로 거래처 위치를 지도에 표시합니다.")
st.markdown(f"데이터 파일: `{CUSTOMER_DATA_FILENAME}` (경로: `{DATA_FOLDER}`)")
st.markdown("---")

df_customers = load_customer_data(CUSTOMER_DATA_FILE_PATH)
last_update_date_str = load_last_update_date(LAST_UPDATE_FILE_PATH)

# --- 사이드바 ---
st.sidebar.header('데이터 관리')
st.sidebar.write(f"데이터 최종 업데이트: {last_update_date_str}")
uploaded_file = st.sidebar.file_uploader(f"'{CUSTOMER_DATA_FILENAME}' 파일 전체 업데이트", type=['xlsx'], key="customer_map_uploader")

if uploaded_file is not None:
    uploaded_file_bytes = uploaded_file.getvalue()
    df_updated, new_update_time = update_customer_data(uploaded_file_bytes, CUSTOMER_DATA_FILE_PATH)
    if df_updated is not None:
        df_customers = df_updated
        last_update_date_str = new_update_time
        st.sidebar.success(f'데이터가 성공적으로 업데이트되었습니다.\n(업데이트 시간: {last_update_date_str})')
        st.experimental_rerun() 

st.sidebar.markdown("---")
st.sidebar.header("거래처 정보 검색 (참고용)")
search_customer_name = st.sidebar.text_input("거래처명으로 검색", key="search_cust_by_name_sidebar")
if search_customer_name and df_customers is not None:
    # 검색 시에는 대소문자 구분 없이, 부분 일치 허용
    searched_by_name_df = df_customers[df_customers['거래처명'].str.contains(search_customer_name.strip(), case=False, na=False, regex=False)]
    if not searched_by_name_df.empty:
        st.sidebar.markdown("**거래처명 검색 결과:**")
        for idx, row in searched_by_name_df.head().iterrows(): # 너무 많은 결과 방지 위해 상위 5개만 표시
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
if search_address and df_customers is not None:
    search_address_stripped = search_address.strip()
    if search_address_stripped: 
        searched_by_address_df_for_map = df_customers[df_customers['주소'].str.contains(search_address_stripped, case=False, na=False, regex=False)]
        if not searched_by_address_df_for_map.empty:
            st.sidebar.markdown(f"**'{search_address_stripped}' 포함 주소 검색 결과 ({len(searched_by_address_df_for_map)}건):**")
            for idx, row in searched_by_address_df_for_map.head().iterrows(): # 상위 결과만 간략히 표시
                st.sidebar.markdown(f"- **{row['거래처명']}**: {row['주소']}")
            if len(searched_by_address_df_for_map) > 5:
                 st.sidebar.caption(f"... 외 {len(searched_by_address_df_for_map) - 5}건 더 있음")
            st.sidebar.markdown("---")
            st.sidebar.info("검색된 거래처들이 지도에 다른 색상으로 표시됩니다.")
        else: 
            st.sidebar.info(f"주소 '{search_address_stripped}'를 포함하는 거래처 정보가 없습니다.")

if df_customers is None or df_customers.empty:
    st.warning("거래처 데이터를 불러올 수 없거나 데이터가 없습니다. 사이드바에서 파일을 업로드하거나 데이터 파일을 확인해주세요.")
    st.stop()

keimeat_row = df_customers[df_customers['거래처명'] == '케이미트']
if not keimeat_row.empty:
    keimeat_coords = (keimeat_row.iloc[0]['위도'], keimeat_row.iloc[0]['경도'])
    map_center = keimeat_coords
    zoom_level = 12
else:
    st.warning("'케이미트' 정보를 찾을 수 없어 거래처 평균 위치로 지도를 표시합니다. '케이미트'를 데이터에 추가해주세요.")
    map_center = (df_customers['위도'].mean(), df_customers['경도'].mean()) 
    zoom_level = 10

groups = {'박용신': 'green', '정종환': 'blue', '이주현': 'purple', '조성균': 'orange', '윤성한': 'yellow'}
m = folium.Map(location=map_center, zoom_start=zoom_level, tiles="cartodbpositron")

if not keimeat_row.empty:
    folium.Marker(
        keimeat_coords,
        icon=folium.Icon(color='red', icon='home', prefix='fa'),
        tooltip='<strong>케이미트 본사</strong>',
        popup=folium.Popup(f"<b>케이미트</b><br>주소: {keimeat_row.iloc[0]['주소']}<br>({keimeat_coords[0]:.4f}, {keimeat_coords[1]:.4f})", max_width=300)
    ).add_to(m)

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

st.sidebar.header("그룹별 배송 루트 설정 (지도 표시)") # 헤더명 변경
selected_customers_to_display = pd.DataFrame()

# '담당자' 컬럼 존재 여부 확인 (냉창 등 다른 정보 표시에 사용)
has_manager_col = MANAGER_COL in df_customers.columns

# 그룹 선택을 위한 전체 일반 거래처 목록 (케이미트 및 차고지 이름 제외)
# 이 목록은 모든 그룹의 multiselect에서 공통으로 사용됩니다.
base_available_customers_df = df_customers[~df_customers['거래처명'].isin(list(groups.keys()) + ['케이미트'])].copy()
all_selectable_customer_names = sorted(list(base_available_customers_df['거래처명'].unique()))

for group_name, color_code in groups.items():
    with st.sidebar.expander(f"{group_name} 그룹 경로 거래처 선택", expanded=False): # 라벨 변경
        if all_selectable_customer_names: # 선택할 수 있는 일반 거래처가 있을 경우
            selected_names_for_group = st.multiselect(
                f'{group_name} 그룹의 배송 거래처를 선택하세요:', # 라벨 변경
                options=all_selectable_customer_names,
                key=f"multiselect_route_{group_name}" # 각 그룹별로 고유한 키 사용
            )
            if selected_names_for_group:
                # 사용자가 선택한 거래처들의 정보를 원본 df_customers에서 가져옴
                group_route_customers_df = df_customers[df_customers['거래처명'].isin(selected_names_for_group)].copy()
                group_route_customers_df['그룹'] = group_name # 지도 표시에 사용할 그룹 정보 추가
                group_route_customers_df['색상'] = color_code # 지도 표시에 사용할 그룹 색상 추가
                selected_customers_to_display = pd.concat([selected_customers_to_display, group_route_customers_df])
        else:
            st.caption("선택할 수 있는 일반 거래처 데이터가 없습니다. (케이미트, 차고지 제외)")


# 지도에 마커 추가 로직
# 1. 주소 검색 결과 마커 추가 (다른 색상으로 표시)
if not searched_by_address_df_for_map.empty:
    for idx, row in searched_by_address_df_for_map.iterrows():
        search_marker_color = 'cadetblue' # 주소 검색 결과 마커 색상
        search_marker_icon = 'info-circle'
        
        # 주소 검색 결과라도 '냉창'이면 특별 표시
        if has_manager_col and row.get(MANAGER_COL, "") == REFRIGERATED_WAREHOUSE_KEYWORD:
            search_marker_color = 'darkblue' # 냉창은 통일된 색상
            search_marker_icon = 'warehouse'
            
        folium.Marker(
            location=(row['위도'], row['경도']),
            tooltip=f"<strong>{row['거래처명']} (주소 검색됨)</strong><br>주소: {row['주소']}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}",
            popup=folium.Popup(f"<b>{row['거래처명']}</b><br>주소: {row['주소']}<br>담당자: {row.get(MANAGER_COL, 'N/A') if has_manager_col else '정보없음'}<br>({row['위도']:.4f}, {row['경도']:.4f})", max_width=300),
            icon=folium.Icon(color=search_marker_color, icon=search_marker_icon, prefix='fa')
        ).add_to(m)

# 2. 그룹 경로 설정 결과 마커 추가
if not selected_customers_to_display.empty:
    for idx, row in selected_customers_to_display.iterrows():
        # 주소 검색 결과에 이미 포함된 경우, 그룹 경로 마커는 중복해서 그리지 않음 (주소 검색 마커가 우선)
        is_already_in_address_search = False
        if not searched_by_address_df_for_map.empty:
            if row['거래처명'] in searched_by_address_df_for_map['거래처명'].values:
                is_already_in_address_search = True
        
        if not is_already_in_address_search: # 주소 검색으로 이미 표시되지 않은 경우에만 그룹 마커 그림
            business_coords = (row['위도'], row['경도'])
            customer_name = row['거래처명']
            customer_address = row['주소']
            group_color_for_marker = row['색상'] # 그룹 경로 설정 시 할당된 색상
            
            marker_color_group_route = group_color_for_marker 
            marker_icon_group_route = 'truck' # 배송 루트 의미하는 아이콘
            
            # 그룹 경로 거래처라도 '냉창'이면 특별 표시
            if has_manager_col and row.get(MANAGER_COL, "") == REFRIGERATED_WAREHOUSE_KEYWORD:
                marker_color_group_route = 'darkblue' # 냉창은 통일된 색상
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
    if not search_address and not any(st.session_state.get(f"multiselect_route_{g}") for g in groups.keys()): # 키 이름 변경 반영
       st.info("사이드바에서 주소로 특정 거래처를 검색하거나, 그룹별로 배송 루트를 설정하면 지도에 표시됩니다. '케이미트' 본사와 각 그룹의 차고지는 기본으로 표시됩니다.")

st_folium(m, width='100%', height=600, returned_objects=[])
