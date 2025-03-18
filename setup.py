from setuptools import setup, find_packages

setup(
    name="hsml_api",
    version="0.1.0",
    author="Alicia Sanjurjo Barrio",
    author_email="alicia.s.barrio@community.isunet.edu",
    description="A Python API for HSML (Hyperspatial Modeling Language).",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/hsml_api",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # Add dependencies here, e.g.,
        # "numpy",
        # "pandas",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)

