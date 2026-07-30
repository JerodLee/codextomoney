#!/usr/bin/env bash
# KBO 팬 앱 — EC2(Ubuntu) 원클릭 설치 스크립트 (Docker 경로)
# 사용법:  bash deploy/setup-ec2.sh
set -euo pipefail

cd "$(dirname "$0")/.."   # kbo-app 루트

echo "▶ 1/3 Docker 확인/설치"
if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y docker.io docker-compose-plugin git openssl
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER" || true
  echo "  (docker 그룹 적용을 위해 재로그인이 필요할 수 있습니다)"
fi

echo "▶ 2/3 관리자 토큰 준비"
if [ -f .admin_token ]; then
  ADMIN_TOKEN="$(cat .admin_token)"
else
  ADMIN_TOKEN="${ADMIN_TOKEN:-$(openssl rand -hex 24)}"
  echo "$ADMIN_TOKEN" > .admin_token
fi
export ADMIN_TOKEN

echo "▶ 3/3 컨테이너 빌드 & 기동"
sudo -E docker compose up -d --build

IP="$(curl -s --max-time 3 ifconfig.me || echo '<EC2-PUBLIC-IP>')"
echo ""
echo "✅ 배포 완료!"
echo "   앱 주소 : http://$IP:3000   (EC2 보안그룹 인바운드 3000/tcp 허용 필요)"
echo "   관리자 토큰(게시판 모더레이션·데이터 API): $ADMIN_TOKEN"
echo "   → 게시판 상단 '🛡 관리자' 버튼에 이 토큰을 넣으면 글/댓글 삭제·숨김해제가 가능합니다."
