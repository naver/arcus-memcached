# Userlog Logger Extension

Userlog Logger Extension은 루트 권한 없이도 접근 가능한 파일 기반 로그 기록을 지원하는 확장 모듈이다.

## 실행 방식

Userlog Logger 기능은 memcached 실행 시 extension 옵션(`-X`)을 지정하여 활성화할 수 있다.

```bash
$INSTALL_PATH/bin/memcached -E $INSTALL_PATH/lib/default_engine.so \
                            -X $INSTALL_PATH/lib/userlog_logger.so
```

## 로그 저장 및 관리 방식

Userlog Logger의 저장 및 관리 방식은 다음과 같다.

- **저장 경로**

  로그 파일은 memcached를 실행한 위치를 기준으로 자동 생성되는 ARCUSlog 디렉토리에 저장된다.
  ```bash
  $PWD/ARCUSlog
  ```

- **로그 파일 이름 형식**

  ```bash
  arcus{index}_{YYYY_MM_DD}
  ```
  - `index` : 0~4 사이 값을 가지는 로그 파일 인덱스
  - `YYYY_MM_DD` : 로그 파일 생성 날짜

- **로그 파일 순환**:

  - 최대 5개(0~4)까지 로그 파일을 유지하며, 각 파일은 최대 20MB까지 저장한다.
  - 모든 파일이 용량 한도에 도달하면, 가장 오래된 파일을 삭제하고 해당 이름으로 새 로그 파일을 생성한다.

## 환경 변수 설정

`userlog_logger` 기능은 다음 환경 변수를 통해 로그 출력 동작을 제어할 수 있다.

1. `UserLogRateLimitInterval`

    일정 시간 동안 기록되는 로그 개수를 제한하는 간격(초)
    > ※ 이 값을 0으로 설정하면 로그 제한 기능이 비활성화되어, 개수 제한 없이 로그가 기록된다.
    - **기본값**: 5초
    - **최대값**: 60초
    ```bash
    export UserLogRateLimitInterval=10
    ```

2. `UserLogRateLimitBurst`

    `UserLogRateLimitInterval` 내에 허용되는 최대 로그 개수
    - **기본값**: 200개
    - **최대값**: 50,000개
    ```bash
    export UserLogRateLimitBurst=1000
    ```

3. `UserLogReduction`

    동일한 로그 메시지가 반복될 경우 하나로 압축해 출력하는 기능
    - 연속된 동일 메시지는 1회만 출력하며, 반복 종료 시 반복 횟수를 함께 출력
    - **기본값**: false
    ```bash
    export UserLogReduction=on
    ```
