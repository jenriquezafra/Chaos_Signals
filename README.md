# Chaos-IV Signals

**Chaos-IV Signals** is an experimental project focused on detecting return signals in stock options using chaotic dynamics and machine learning models.

## Objective

The main goal is to develop a quantitative analysis pipeline that combines:

- Nonlinear dynamics and chaos metrics (Hurst exponent, permutation entropy, Lyapunov exponents)
- Latent factor extraction from implied volatility surfaces (PCA, variational autoencoders)
- Supervised learning models (e.g., logistic regression, XGBoost)
- Backtesting to evaluate predictive performance

## Structure
- `notebooks/`: -> Jupyter notebooks for exploration modeling.
- `src/`: Reusable python modules.
- `data/`: Raw and processed option data.
- `outputs/`: Visualization and signal results.
