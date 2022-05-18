import random


class Markov:
    def __init__(self, corpus, ngram=3):
        self.cache = {}
        self.ngram = ngram
        self.words = corpus.split()
        self.word_size = len(self.words)
        self.database()

    def generate_ngrams(self):
        if len(self.words) < self.ngram:
            return

        for i in range(len(self.words) - self.ngram + 1):
            yield (self.words[i : i + self.ngram])

    def database(self):
        for w in self.generate_ngrams():
            sep = min(2, self.ngram - 1)
            key = tuple(w[:sep])
            remain = w[sep:][0]

            if len(key) == 1:
                key = key[0]

            if key in self.cache:
                self.cache[key].append(remain)
            else:
                self.cache[key] = [remain]

    def generate_markov_text(self, size=20):
        seed = random.randint(0, self.word_size - 3)
        gen_words = []

        try:
            if self.ngram <= 2:
                seed_word = self.words[seed]
                w1 = seed_word

                for i in range(size):
                    gen_words.append(w1)
                    w1 = random.choice(self.cache[w1])

            else:
                seed_word, next_word = self.words[seed], self.words[seed + 1]
                w1, w2 = seed_word, next_word

                for i in range(size):
                    gen_words.append(w1)
                    w1, w2 = w2, random.choice(self.cache[(w1, w2)])

                gen_words.append(w2)
        except Exception as e:
            print(e)
            pass

        return " ".join(gen_words)
