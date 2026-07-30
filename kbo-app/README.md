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

## EC2 배포 — 원클릭 스크립트 (가장 쉬움)

```bash
git clone <이 저장소> && cd codextomoney/kbo-app
bash deploy/setup-ec2.sh
# Docker 설치→토큰 생성→빌드→기동까지 자동. 끝나면 접속 주소와 관리자 토큰을 출력합니다.
```

## EC2 배포 — 방법 A: Docker (수동)

```bash
# EC2(Ubuntu)에서
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
git clone <이 저장소> && cd codextomoney/kbo-app
export ADMIN_TOKEN=$(openssl rand -hex 24)   # 게시판 모더레이션·데이터 갱신 API 보호용
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
| POST | `/api/board/:id/report` | 글 신고 (누적 `AUTO_HIDE`회 시 자동 숨김) |
| POST | `/api/board/:id/comment/:idx/report` | 댓글 신고 |
| DELETE | `/api/board/:id` | 글 삭제 (`x-admin-token`) |
| DELETE | `/api/board/:id/comment/:idx` | 댓글 삭제 (`x-admin-token`) |
| POST | `/api/board/:id/unhide` | 숨김 해제 (`x-admin-token`) |
| GET | `/api/data` | 저장된 KBO 데이터셋 |
| POST | `/api/data` | 데이터셋 저장 (헤더 `x-admin-token` 필요) |

- 스팸 방지: IP당 분당 8건 작성 제한, 본문 길이 제한, 서버 저장 후 **프론트에서 출력 시 이스케이프**.
- 데이터 정기 갱신: 앱의 "데이터 관리" 탭에서 편집 → JSON 내보내기 → `POST /api/data`로 반영하거나,
  cron으로 최신 데이터를 만들어 같은 API에 올리면 모든 사용자에게 공유됩니다.

## 모더레이션 (게시판 운영)

- **신고 → 자동 숨김**: 누구나 글/댓글을 신고할 수 있고, 신고가 `AUTO_HIDE`(기본 5)회 누적되면 자동으로 숨겨집니다.
- **관리자 삭제/숨김해제**: 게시판 상단 **🛡 관리자** 버튼에 `ADMIN_TOKEN` 을 입력하면 삭제·숨김해제 버튼이 나타납니다(토큰은 브라우저에만 저장, 요청 헤더로 전송).
- **금칙어 마스킹**: `BANNED_WORDS="단어1,단어2"` 환경변수에 넣은 단어는 작성 시 `***` 로 치환됩니다.
- 설정 예: `AUTO_HIDE=5 BANNED_WORDS="비속어1,비속어2" ADMIN_TOKEN=... docker compose up -d`

## 데이터 자동 갱신 (cron / 타이머)

경기결과·성적을 주기적으로 서버에 반영합니다. 실제 최신 데이터는 `DATA_SOURCE_URL`(JSON 피드)로 연결하세요.

```bash
# 수동 실행
ADMIN_TOKEN=xxx DATA_SOURCE_URL=https://your-feed/kbo.json node scripts/update-data.mjs
# 또는 로컬 파일에서:  SEED_FILE=./data/seed.json node scripts/update-data.mjs
```

**systemd 타이머(매일 07:00):**
```bash
sudo cp deploy/kbo-update.* /etc/systemd/system/
sudo sed -i "s/change-me/$(cat .admin_token)/" /etc/systemd/system/kbo-update.service
sudo systemctl daemon-reload && sudo systemctl enable --now kbo-update.timer
```

**crontab 대안:**
```
0 7 * * * cd /opt/kbo-app && ADMIN_TOKEN=xxx DATA_SOURCE_URL=... node scripts/update-data.mjs >> /var/log/kbo-update.log 2>&1
```

> 앱의 "데이터 관리" 탭에서 JSON 내보내기 → 그 파일을 `SEED_FILE`로 지정하면, 손으로 편집한 데이터를 그대로 서버에 반영할 수도 있습니다.

## 문제 해결 (Troubleshooting)

- **접속이 안 됨**: EC2 **보안 그룹** 인바운드에 `3000/tcp`(또는 nginx 사용 시 80/443)가 열려 있는지 확인.
- **`docker: permission denied`**: `sudo usermod -aG docker $USER` 후 재로그인, 또는 `sudo docker ...`.
- **포트 충돌(3000)**: `PORT=8080 docker compose up -d` 처럼 포트 변경.
- **게시글이 사라짐/초기화**: 데이터는 `./data` 볼륨에 저장됩니다. 컨테이너를 지워도 `data/`는 보존되며, `data/`까지 지우면 초기화됩니다.
- **관리자 기능이 안 보임**: `ADMIN_TOKEN`이 설정된 상태로 서버가 떠 있어야 하고, 게시판 🛡 버튼에 같은 토큰을 입력해야 합니다.
- **데이터 갱신 401**: `update-data.mjs`의 `ADMIN_TOKEN`이 서버의 값과 일치해야 합니다.
- 로그 확인: `docker compose logs -f`  /  systemd: `journalctl -u kbo-board -f`.
