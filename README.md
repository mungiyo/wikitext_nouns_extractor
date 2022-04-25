# wikitext_nouns_extractor

wiki_extractor를 통해 위키 텍스트 데이터를 추출한 파일에서 line 당 명사를 추출하는 스크립트입니다.

## Preprocessing
WikiExtractor를 이용하여 Raw Data를 전처리한 후 사용.
- [Raw Data](https://dumps.wikimedia.org/kowiki/latest/kowiki-latest-pages-articles.xml.bz2)
- [WikiExtractor](https://github.com/attardi/wikiextractor.git)

## How it work
1. Data를 BUFFER_SIZE로 쪼갠다.
2. 하나의 segment를 CHUNK_SIZE로 쪼갠 후 각 프로세서에 할당 후 비동기로 전처리한다.
3. 하나의 segment가 처리되면 다음 segment에 대해 2를 수행한다.
