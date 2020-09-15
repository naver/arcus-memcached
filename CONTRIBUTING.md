## Contributing
Contribution 하기 위한 선행 작업으로 CLA(Contributor License Agreement)에 대해
contributor의 sign이 필요합니다. 먼저, 아래 Apache CLA 내용을 자세히 읽어보시기 바랍니다.
Apache CLA에 동의하시면 해당 CLA에 필수 사항 기재하고 sign하고 나서, singed CLA를 스캔한 이미지를
junhyun.park@jam2in.com 메일 계정으로 보내 주시기 바랍니다.

- [Apache ICLA(Individual Contributor License Agreement)](http://www.apache.org/licenses/icla.pdf)
- [Apache CCLA(Corporate Contributor License Agreement)](http://www.apache.org/licenses/cla-corporate.txt)

Contribution은 GitHub pull requests 방식을 사용해 주시기 바랍니다.

## Issues
ARCUS-memcached 와 관련된 모든 사항(questing, bug report, feature proposal 등)을 자유롭게 issue로 등록 해 주세요.
새로운 issue를 생성하기 전 아래 사항을 고려해 주세요.
- 생성하려는 issue가 이미 해결 되었는지 확인 하세요.
- build가 실패했다면, [Travis build status](https://travis-ci.org/naver/arcus-memcached) 를 보고 현재 빌드 상태를 확인하세요.
- 문제가 해결되지 않으면, 최대한 자세하게 설명 해주세요. 더 많은 정보를 제공할수록 빠른 해결이 가능합니다.

## Pull Requests
ARCUS-memcached 의 최신 코드는 develop branch에 유지됩니다. 모든 PR은 develop branch를 기반으로 작성되어야 합니다.
PR 작성 전에 다음 사항을 고려해 주세요.
- 모든 변경은 develop branch를 기반으로 변경하고, develop branch로 요청 해주세요.
- 모든 코드 파일에는 라이센스 주석 사본이 있어야 합니다. 기존 파일에서 복사할 수 있습니다.
- 변경 범위가 큰 이슈일 경우 include/memcached/types.h 파일에 코드 태그를 생성하고 해당 태그를 이용하여 변경 코드들을 감싸주세요.
- 새로운 기능을 추가하는 경우 해당 기능을 시험하는 unit test를 함께 작성해주세요.
- PR 전 make test로 수행되는 unit test를 포함한 기본 테스트를 진행 해주세요.
- 모든 소스 코드는 C coding style 를 기준으로 작성 해주시고, 같은 파일 내의 코드 수정은 파일 내 style을 따라주세요.
- Commit message 작성은 "Classification: commit message" 형식을 지켜주세요.

  |Classification|내용|
  |:-------------|:----|
  |FIX|fix bugx|
  |FEATURE|new feature|
  |ENHANCE|enhancement or optimization|
  |CLEANUP|code cleanup, refactoring|
  |DOC|document related commit|
  |LEGAL|copylight, license 변경|
  |VERSION|version 변경|
  |TEST|add/remove/update tests|

- 모든 PR은 담당자의 리뷰 후 반영됩니다. PR의 수정이 필요 할 경우 수정을 요청할 수 있습니다.

## Public Documents
- [ARCUS 도입기](https://www.slideshare.net/JaM2in/arcus-190620954), ARCUS User Meetup 2019 Autumn
- [ARCUS Persistence](https://www.slideshare.net/JaM2in/arcus-persistence), ARCUS User Meetup 2019 Autumn
- [ARCUS & JaM2in, Replication, Java Client](https://www.slideshare.net/JaM2in/arcus-offline-meeting-2015-05-20-1), Offline metting 2015
- [ARCUS 차별 기능, 사용 이슈 그리고 카카오 적용 사례](https://www.slideshare.net/JaM2in/arcus-offline-meeting-2015-05-20-1), DEVIEW 2014
- [웹서비스 성능향상을 위한 오픈소스 Arcus 주요 기능과 활용 사례](https://www.slideshare.net/deview/2b3arcus), 네이버 오픈세미나 at 광주
- [Arcus Collection 기능과 Open Source 전략](https://www.slideshare.net/deview/3aruc), Open Technet Summit 2014
- [Arcus: NHN Memcached Cloud](https://www.slideshare.net/sdec2011/sdec2011-arcus-nhn-memcached-cloud-8467157), SDEC 2011
