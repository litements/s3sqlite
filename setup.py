import setuptools
from pathlib import Path


setuptools.setup(
    name="s3sqlite",
    version="0.2",
    author="Ricardo Ander-Egg Aguilar",
    author_email="rsubacc@gmail.com",
    description="Query SQLite databases on S3 using s3fs",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/litements/s3sqlite",
    py_modules=["s3sqlite"],
    classifiers=[
        "Operating System :: OS Independent",
    ],
    install_requires=["fsspec", "apsw", "s3fs", "boto3"],
    python_requires=">=3.7",
)
