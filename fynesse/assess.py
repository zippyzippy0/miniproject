# fynesse/assess.py
import numpy as np
from scipy.stats import bernoulli, norm, pearsonr
from sklearn.linear_model import LinearRegression

def bernoulli_access(distances_km, threshold_km=5):
    """
    Calculate probability of access to a facility within a threshold distance.
    
    Parameters:
        distances_km (array-like): Distances to nearest facility in km.
        threshold_km (float): Distance threshold (default=5 km).
        
    Returns:
        prob_access (float): Probability of having facility within threshold.
        outcome (np.ndarray): Bernoulli outcome array (1 if within threshold, else 0).
    """
    distances_km = np.array(distances_km)
    outcome = (distances_km <= threshold_km).astype(int)
    prob_access = outcome.mean()
    return prob_access, outcome

def gaussian_distance_analysis(distances_km):
    """
    Fit a Gaussian distribution to distances.
    
    Parameters:
        distances_km (array-like): Distances to nearest facility in km.
        
    Returns:
        mu (float): Mean of distances.
        sigma (float): Standard deviation of distances.
        pdf (np.ndarray): Probability density function values for each distance.
    """
    distances_km = np.array(distances_km)
    mu, sigma = np.mean(distances_km), np.std(distances_km)
    pdf = norm.pdf(distances_km, mu, sigma)
    return mu, sigma, pdf

def bayesian_regression(X, y):
    """
    Simple linear regression using least squares (can be used as a Bayesian approx).
    
    Parameters:
        X (array-like): Predictor(s), shape (n_samples, n_features)
        y (array-like): Target variable, shape (n_samples,)
        
    Returns:
        coef (np.ndarray): Regression coefficients.
    """
    X = np.array(X)
    y = np.array(y)
    coef = np.linalg.lstsq(X, y, rcond=None)[0]
    return coef

def pearson_correlation(x, y):
    """
    Compute Pearson correlation coefficient and p-value.
    
    Parameters:
        x (array-like)
        y (array-like)
        
    Returns:
        r (float): Pearson correlation coefficient.
        p (float): Two-tailed p-value.
    """
    x = np.array(x)
    y = np.array(y)
    r, p = pearsonr(x, y)
    return r, p

def linear_model_r2(X, y):
    """
    Fit linear regression and compute R² score.
    
    Parameters:
        X (array-like): Predictor(s), shape (n_samples, n_features)
        y (array-like): Target variable, shape (n_samples,)
        
    Returns:
        r2 (float): R² score.
        y_pred (np.ndarray): Predicted values from the model.
    """
    X = np.array(X)
    y = np.array(y)
    model = LinearRegression().fit(X, y)
    y_pred = model.predict(X)
    r2 = model.score(X, y)
    return r2, y_pred

def normalize_column(df, col_name):
    """
    Normalize a column as percentage of total sum.
    
    Parameters:
        df (pd.DataFrame): Input dataframe.
        col_name (str): Column to normalize.
        
    Returns:
        pd.Series: Normalized values (% of total).
    """
    return df[col_name] / df[col_name].sum() * 100
