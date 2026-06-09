"""서울특별시 25개 자치구의 법정동 시군구코드(5자리).

국토교통부 실거래가 API의 LAWD_CD 파라미터로 사용된다.
"""
from __future__ import annotations

# {자치구명: 법정동시군구코드}
SEOUL_DISTRICTS: dict[str, str] = {
    "종로구": "11110",
    "중구": "11140",
    "용산구": "11170",
    "성동구": "11200",
    "광진구": "11215",
    "동대문구": "11230",
    "중랑구": "11260",
    "성북구": "11290",
    "강북구": "11305",
    "도봉구": "11320",
    "노원구": "11350",
    "은평구": "11380",
    "서대문구": "11410",
    "마포구": "11440",
    "양천구": "11470",
    "강서구": "11500",
    "구로구": "11530",
    "금천구": "11545",
    "영등포구": "11560",
    "동작구": "11590",
    "관악구": "11620",
    "서초구": "11650",
    "강남구": "11680",
    "송파구": "11710",
    "강동구": "11740",
}

CODE_TO_DISTRICT: dict[str, str] = {v: k for k, v in SEOUL_DISTRICTS.items()}


def district_code(name: str) -> str | None:
    return SEOUL_DISTRICTS.get(name)


def district_name(code: str) -> str | None:
    return CODE_TO_DISTRICT.get(code)
