from setuptools import setup, find_packages

setup(
    name='cubist-hackathon',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'polars',
        'httpx',
        'csp',
    ],
    python_requires='>=3.10',
    entry_points={
        'console_scripts': [
            'cubist-hackathon=main:main',
            'cubist-hackathon=tests.test_pipeline:main',
            'cubist-hackathon=tests.test_graph:main',
        ],
    },
)
