"""주기 모니터링 실행 스크립트.

cron / GitHub Actions에서 실행하면 현재 구역 단계·시세 스냅샷을 만들고
직전 스냅샷과 비교해 변동을 backend/state/*.json 에 누적한다.
(텔레그램 환경변수가 있으면 알림도 발송)
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(BACKEND))

from services.monitor import capture_snapshot  # noqa: E402

if __name__ == "__main__":
    result = capture_snapshot()
    print(f"snapshot at {result['snapshot_at']}: "
          f"{result['areas']} areas, {result['new_changes']} new change(s)")
