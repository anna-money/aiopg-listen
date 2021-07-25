import re
from pathlib import Path

from setuptools import setup

install_requires = ["aiopg>=1.3.1", "async_timeout>=3.0,<4.0"]


def read(*parts):
    return Path(__file__).resolve().parent.joinpath(*parts).read_text().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*\"([\d.abrc]+)\"")
    for line in read("aiopg_listen", "__init__.py").splitlines():
        match = regexp.match(line)
        if match is not None:
            return match.group(1)
    else:
        raise RuntimeError("Cannot find version in aiopg_listen/__init__.py")


long_description_parts = []
with open("README.md", "r") as fh:
    long_description_parts.append(fh.read())

with open("CHANGELOG.md", "r") as fh:
    long_description_parts.append(fh.read())

long_description = "\r\n".join(long_description_parts)


setup(
    name="aiopg-listen",
    version=read_version(),
    description="Helps to use PostgreSQL listen/notify with aiopg",
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms=["macOS", "POSIX", "Windows"],
    author="Yury Pliner",
    python_requires=">=3.8",
    project_urls={},
    url="https://github.com/Pliner/aiopg-listen",
    author_email="yury.pliner@gmail.com",
    license="MIT",
    packages=["aiopg_listen"],
    package_dir={"aiopg_listen": "./aiopg_listen"},
    package_data={"aiopg_listen": ["py.typed"]},
    install_requires=install_requires,
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
        "Environment :: Web Environment",
        "Development Status :: 5 - Production/Stable",
        "Framework :: AsyncIO",
        "Typing :: Typed",
    ],
)
