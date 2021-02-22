"""Functions that process and return pandas.DataFrames"""

# PyPI libraries
import numpy
import pandas

# Helper functions

def corr2cov(corr: numpy.ndarray, stdevs: numpy.ndarray) -> numpy.ndarray:
    """Covariance matrix from correlation & standard deviations
    @param corr: correlation matrix
    @param stdevs: standard deviations

    See https://realpython.com/python-random/
    """
    diag_stdevs = numpy.diag(stdevs)
    cov = diag_stdevs @ corr @ diag_stdevs
    return cov

# DAG tasks

def get_simulated_dataset(n_row: int=10000) -> pandas.DataFrame:
    numpy.random.seed(20210221)
    val_normal = numpy.random.randn(n_row)
    p_unif = numpy.random.rand(n_row)

    mean = numpy.array([0.0, 0.0])
    cov = corr2cov(
        corr=numpy.array([
            [1.0, -0.3],
            [-0.3, 1.0]
        ]),
        stdevs=numpy.array([1.0, 1.0])
    )
    data_correlated = numpy.random.multivariate_normal(mean=mean, cov=cov, size=n_row)
    val_multivariate_normal_1 = data_correlated[:, 0]
    val_multivariate_normal_2 = data_correlated[:, 1]

    val_error = 5 * numpy.random.randn(n_row)

    val_response = 2 * val_normal - 3 * p_unif + 4 * val_multivariate_normal_1 + 5 * val_multivariate_normal_2 + val_error

    return pandas.DataFrame({
        'val_response': val_response,
        'p_unif': p_unif,
        'val_normal': val_normal,
        'val_multivariate_normal_1': val_multivariate_normal_1,
        'val_multivariate_normal_2': val_multivariate_normal_2
    })

def get_transformed_dataset(dataset):
    return dataset
