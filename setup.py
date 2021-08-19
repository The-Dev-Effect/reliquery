from setuptools import setup

install_requires = [
    "numpy >= 1.16",
    "boto3 >= 1.17",
    "black",
    "pytest"
]

setup(
    name="reliquery",
    version="0.1.0",
    description="Science's Artifact Antiformat",
    url="https://github.com/The-Dev-Effect/reliquery",
    author="The Dev Effect",
    author_email="company@thedeveffect.com",
    license="MIT OR Apache-2.0",
    packages=["reliquery"],
    tests_require=["pytest"],
    install_requires=install_requires,
)
