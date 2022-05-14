from imblearn.over_sampling import SMOTE
import numpy as np


class EzSMOTE:
    def __init__(self, samples):
        self.samples = samples

    def generate_data(self, n=20):
        sm = SMOTE()

        T1 = np.array(self.samples)
        T2 = np.random.rand(len(self.samples) + n)
        y1 = [0] * len(T1)
        y2 = [1] * len(T2)

        X = np.concatenate((T1, T2))
        X = [[x] for x in X]
        y = y1 + y2

        X_generated, _ = sm.fit_resample(X, y)
        X_generated = X_generated[len(self.samples) :]
        return X_generated
