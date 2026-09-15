import sys
import itertools

def word_count(filename: str, k: int) -> dict:
    counts = {}
    with open(filename) as f:
        for line in f:
            for word in line.split():
                counts[word] = counts[word]+1 if word in counts else 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True)[:k])

if __name__ == '__main__':
    filename = "../data/hamlet.txt" if len(sys.argv) < 2 else sys.argv[1]
    result =  word_count(filename, 10)
    [print(key, ":", result[key]) for key in result]
