# setup.py
from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='modules',
    version='0.0.1',
    packages=find_packages(),
    description='LLM based recommendation system',
    install_requires=[
        requirements
    ],
)