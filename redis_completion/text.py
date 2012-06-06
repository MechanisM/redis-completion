import re


def clean_phrase(phrase, stop_words=None):
    phrase = phrase.lower()
    stop_words = stop_words or set()
    return [w for w in phrase.split() if w not in stop_words]

def partial_complete(phrase, min_words=2, max_words=3, stop_words=None):
    words = clean_phrase(phrase, stop_words)

    max_words = max(
        min(len(words), max_words), min_words
    )

    wc = len(words)
    for i in range(wc):
        if max_words + i > wc:
            yield ' '.join(words[i:i+min_words])
        else:
            for ct in range(min_words, max_words + 1):
                yield ' '.join(words[i:i+ct])

def create_key(phrase, max_words=3, stop_words=None):
    key = ' '.join(clean_phrase(phrase, stop_words)[:max_words])
    return re.sub('[^a-z0-9_-]', '', key)
