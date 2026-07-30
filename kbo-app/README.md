# KBO 팬 앱 (EC2 배포용 풀스택)

전력분석·경기예측 보드에 **초보팬 가이드**와 **팬 게시판(훌리건 배틀)** 을 더한 풀스택 앱입니다.

- **프론트엔드**: `public/index.html` (단일 파일 SPA — 순위·구단/선수·상대전적·일정·뉴스·시뮬·데이터 관리·초보팬 가이드·게시판)
- **백엔드**: `server.js` (Node/Express) — 정적 서빙 + 게시판 API + 데이터 동기화 API
- **저장소**: `data/board.json`, `data/dataset.json` (순수 JSON, 네이티브 의존성 없음)

게시판은 백엔드가 있으면 **모두가 함께 쓰는 실시간 게시판**, 백엔드 없이 파일만 열면 **내 브라우저 로컬 미리보기 모드**로 동작합니다.

---

## 로컬 실행

```bash
cd kbo-app
npm install
node server.js
# http://localhost:3000
```

## EC2 배포 — 방법 A: Docker (권장)

```bash
# EC2(Ubuntu)에서
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
git clone <이 저장소> && cd codextomoney/kbo-app
export ADMIN_TOKEN=$(openssl rand -hex 24)   # 데이터 갱신 API 보호용
sudo docker compose up -d --build
# 앱: http://<EC2퍼블릭IP>:3000
```
- EC2 **보안 그룹**에서 인바운드 3000(또는 80/443) 포트를 열어주세요.
- 데이터는 `./data` 볼륨에 유지됩니다(컨테이너 재시작에도 보존).

## EC2 배포 — 방법 B: Node + systemd + nginx

```bash
# Node 20 설치
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - && sudo apt install -y nodejs nginx
sudo mkdir -p /opt/kbo-app && sudo cp -r kbo-app/* /opt/kbo-app/ && cd /opt/kbo-app
sudo npm install --omit=dev

# systemd 서비스
sudo cp deploy/kbo-board.service /etc/systemd/system/
sudo sed -i "s/change-me/$(openssl rand -hex 24)/" /etc/systemd/system/kbo-board.service
sudo systemctl daemon-reload && sudo systemctl enable --now kbo-board

# nginx 리버스 프록시 (80 -> 3000)
sudo cp deploy/nginx.conf /etc/nginx/sites-available/kbo
sudo ln -sf /etc/nginx/sites-available/kbo /etc/nginx/sites-enabled/kbo
sudo nginx -t && sudo systemctl reload nginx
```
- 도메인이 있으면 `nginx.conf`의 `server_name`을 도메인으로 바꾸고 HTTPS 적용:
  ```bash
  sudo apt install -y certbot python3-certbot-nginx
  sudo certbot --nginx -d your-domain.com
  ```

---

## API 요약

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/health` | 서버 상태 |
| GET | `/api/board?team=all&sort=new` | 글 목록(구단 필터·정렬 new/hot) |
| GET | `/api/board/:id` | 글 + 댓글 |
| POST | `/api/board` | 글 작성 `{team,nick,title,body}` |
| POST | `/api/board/:id/comment` | 댓글 `{nick,body}` |
| POST | `/api/board/:id/vote` | 추천/비추천 `{dir:1\|-1}` (IP당 1표) |
| GET | `/api/data` | 저장된 KBO 데이터셋 |
| POST | `/api/data` | 데이터셋 저장 (헤더 `x-admin-token` 필요) |

- 스팸 방지: IP당 분당 8건 작성 제한, 본문 길이 제한, 서버 저장 후 **프론트에서 출력 시 이스케이프**.
- 데이터 정기 갱신: 앱의 "데이터 관리" 탭에서 편집 → JSON 내보내기 → `POST /api/data`로 반영하거나,
  cron으로 최신 데이터를 만들어 같은 API에 올리면 모든 사용자에게 공유됩니다.

## 운영 주의 (게시판)

공개 게시판은 이용자 작성물이 그대로 노출됩니다. 과도한 비방·개인정보·불법 콘텐츠에 대비해
운영자 모더레이션(삭제) 정책을 두는 것을 권장합니다. 필요하면 삭제용 관리자 API를 추가해 드릴 수 있습니다.
