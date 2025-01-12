from multiprocessing import Pool
import re
from collections import Counter
from nltk.corpus import stopwords
from functools import reduce
import time
import nltk

# Загрузка стоп-слов
nltk.download('stopwords')

def clean_word(word):
    cleaned_word = re.sub(r'[^a-zA-Z]', '', word).lower()
    return cleaned_word if cleaned_word else None

def word_not_in_stopwords(word):
    return word is not None and word not in stopwords.words('english') and word.isalpha()

def mapper(words):
    tokens_in_text = map(clean_word, words)
    tokens_in_text = filter(word_not_in_stopwords, tokens_in_text)
    return Counter(tokens_in_text)

def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1

def chunkify(data, number_of_chunks):
    """Делит данные на заданное количество чанков."""
    k, m = divmod(len(data), number_of_chunks)
    return (data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_chunks))

def chunk_mapper(chunk):
    """Обрабатывает чанк данных (частоту слов)."""
    return mapper(chunk)

def open_file():
    with open("Text.txt", "r") as file:
        content = file.read()
    return content.split()

def save_to_file(result, filename="result.txt"):
    """Сохраняет результат в файл."""
    with open(filename, "w") as file:
        for word, count in result.most_common():
            file.write(f"{word}: {count}\n")

if __name__ == "__main__":
    start_time = time.time()

    words = open_file()
    number_of_chunks = 10

    data_chunks = chunkify(words, number_of_chunks)

    with Pool() as pool:
        mapped = pool.map(chunk_mapper, data_chunks)

    result = reduce(reducer, mapped)

    # Вывод результата в консоль
    print(result)

    # Сохранение результата в файл
    save_to_file(result)

    end_time = time.time()
    print(f"Время выполнения: {end_time - start_time:.2f} секунд")