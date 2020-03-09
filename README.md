# Didomi Data Challenge

The repository is my answer to the Didomi Data Challenge.

## 0 - Requirements

For the development environment, you could this docker image containing:

- Python 3.7
- PySpark
- Jupyter Notebook (useful for experiments on data)
- Pytest suite (Pytest-Watch and Pytest-Coverage)

Although, this solution is not efficient. The unit tests requires to start a new SparkContext at every run, which increase significantly the waiting time.

To build the image:

```sh
make build
```

## 1 - Run the Challenge

Depending on your need, there are a few configured `make` options available. Don't hesitate to open more terminals if needed.

To simply run the spark job:

    make run

To launch pytest-watch, so unit tests are run at every file saving:

    make dev

To launch a Jupyter Notebook server. Simply on the last link displayed:

    make notebook

To evaluate the test coverage:

    make coverage

To open a terminal shell from the container to run commands:

    make bash