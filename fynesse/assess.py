import numpy as np
import math
from scipy.stats import bernoulli, norm


def bernoulli_access(distances, threshold_km):
    return bernoulli.rvs(distances <= threshold_km)


def gaussian_distance_analysis(distances):
    mu, sigma = np.mean(distances), np.std(distances)
    pdf = norm.pdf(distances, mu, sigma)
    return mu, sigma, pdf


def bayesian_regression(X, y):
    coef = np.linalg.lstsq(X, y, rcond=None)[0]
    return coef
