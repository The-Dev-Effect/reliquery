from setuptools import setup

install_requires = [
    "numpy >= 1.16",
    "boto3 >= 1.17",
    "black>=21.4b",
    "pytest",
    "flake8",
    "sphinx",
]

setup(
    name="reliquery",
    version="0.2.6",
    description="Science's Artifact Antiformat",
    url="https://github.com/The-Dev-Effect/reliquery",
    author="The Dev Effect",
    author_email="company@thedeveffect.com",
    license="MIT OR Apache-2.0",
    packages=["reliquery"],
    tests_require=["pytest"],
    install_requires=install_requires,
)
