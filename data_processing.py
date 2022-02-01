from concurrent import futures
from io import TextIOWrapper
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from tqdm import tqdm
from typing import List
import time
import re

BUFFER_SIZE = 1024 * 1024 * 500 # 500 MB
CHUNK_SIZE = 100000

file_in = 'wikitext.txt'
file_out = 'processed_text.txt'

def text_generator(file_object: TextIOWrapper):
    while True:
        lines = file_object.readlines(BUFFER_SIZE)
        if not lines: break
        yield lines

def segment_chunk(segment: List[str], chunk_size: int):
    return [segment[i:i+chunk_size] for i in range(0, len(segment), chunk_size)]

def text_processing(segment: List[str]):
    p = re.compile("^</?doc")
    processed_lines = []

    for line in segment:
        line = line.strip()
        if p.match(line) or line is None:
            continue
        nouns = [word for word, tag in pos_tag(word_tokenize(line)) if tag == 'NN']
        processed_lines.append(nouns)

    tmp = [','.join(nouns) for nouns in processed_lines]
    data = '\n'.join(tmp)

    with open(file_out, 'a', encoding='UTF-8') as f:
        f.write(data)
    
    # print(f"{len(segment)} lines processing complete.")

if __name__ == "__main__":
    processed_line = 0
    with open(file_in, 'r', encoding='UTF-8') as file:
        # text BUFFER_SIZE 만큼 분할 -> 분할된 리스트 형태
        # ex) BUFFER_SIZE=1GB -> [1GB, 1GB, ..., 나머지 B]
        segments = text_generator(file)
        start_time = time.time()
        # 전처리된 데이터를 쓸 파일 초기화
        with open(file_out, 'w', encoding='UTF-8'):
            pass
        # 분할된 text의 CHUNK_SIZE 만큼 분할 시켜서 각 프로세스에 할당
        # ex) segment=1GB -> CHUNK_SIZE 만큼 쪼개서 각 프로세스에 할당
        # chunked_segment -> [CHUNK_SIZE, CHUNK_SIZE, ..., 나머지]
        for segment in segments:
            s_time = time.time()
            chunked_segment = segment_chunk(segment, CHUNK_SIZE)
            with futures.ProcessPoolExecutor() as executor:
                executor.map(text_processing, tqdm(chunked_segment))
            processed_line += len(segment)
            print(f"현재 처리된 라인 수 : {processed_line}")
            print(f"1GB 처리 소요 시간 : {time.time() - s_time} sec")

        print(f"총 소요 시간 : {time.time() - start_time} sec")